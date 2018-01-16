/*-------------------------------------------------------------------------
 *
 * garbagemap.c - Garbagemap generator for heap relations
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  walker/garbagemap.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "walker.h"

/* These are always necessary for a bgworker */
#include "access/relscan.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/transam.h"
#include "access/heapam_xlog.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#define	GARBAGE_SUMMARY_COLS 5

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(garbage_summary);

/* Garbagemap array size */
#define MAP_SIZE 1024

/*
 * Each MAP_RANGE_SIZE blocks belong to same index.
 * If a bit of visibility map is set, we at least read
 * SKIP_PAGES_THRESHOLD blocks (currently 32 blocks).
 * So it makes sence if we have ranges per more than
 * 32 blocks.
 */
#define MAP_RANGE_SIZE (32 * 4)

/* Convert target block number to index of heatmap */
#define TargetBlknoToIndex(blkno) (blkno / MAP_RANGE_SIZE)

typedef struct Garbagemap
{
	RelFileNode	node;
	int			hmap[MAP_SIZE];
} Garbagemap;

typedef struct RangStat
{
	int32	rangeno;
	uint32	freespace;
	uint32	n_tuples;
	uint32	n_dead_tuples;
	uint32	n_all_visible;
} RangeStat;
static int32 summary_size;

/* Plugin handler function */
extern void _PG_walker_plugin_init(WALkerCallbacks *cb);

void		_PG_init(void);

/* Callback functions */
static void heatmap_startup(void);
static void heatmap_heap(XLogReaderState *record);
static void heatmap_heap2(XLogReaderState *record);
static void heatmap_xlog(XLogReaderState *record);
static void heatmap_xact(XLogReaderState *record);
static void heatmap_smgr(XLogReaderState *record);

/* Garbagemap processing functions */
static void GarbagemapChangeTemprature(RelFileNode node, BlockNumber blkno,
									int delta_temp);
static void GarbagemapCoolTemprature(RelFileNode node, BlockNumber blkno,
								  int ratio);
static void GarbagemapClearTemprature(RelFileNode node);
static Garbagemap *GetGarbagemapEntry(RelFileNode node);

static void put_tuple(Tuplestorestate *tupstore, TupleDesc tupdesc, RangeStat stat);

/* Debug purpose */
static void GarbagemapLogSummary(void);
static char *GarbagemapDump(int *heatmap);

static HTAB *GarbagemapHash;

void
_PG_init(void)
{
	DefineCustomIntVariable("garbagemap.summary_size",
							"The number of blocks for summarizing",
							NULL,
							&summary_size,
							320, /* 32 * 10 blocks */
							1,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
}

static void
put_tuple(Tuplestorestate *tupstore, TupleDesc tupdesc, RangeStat stat)
{
	Datum values[GARBAGE_SUMMARY_COLS];
	bool nulls[GARBAGE_SUMMARY_COLS];

	/* Range number */
	values[0] = Int32GetDatum(stat.rangeno);
	/* Freespace */
	values[1] = Int32GetDatum(stat.freespace);
	/* Total tuples */
	values[2] = Int32GetDatum(stat.n_tuples);
	/* Total dead tuples */
	values[3] = Int32GetDatum(stat.n_dead_tuples);
	/* All visible pages */
	values[4] = Int32GetDatum(stat.n_all_visible);

	MemSet(&nulls[0], false, GARBAGE_SUMMARY_COLS);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

Datum
garbage_summary(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HeapScanDesc	scan;
	Relation		rel;
	TupleDesc		tupdesc;
	Tuplestorestate *tupstore;
	HeapTuple		tuple;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	BlockNumber		prev_blkno = InvalidBlockNumber;
	RangeStat		stat;
	SnapshotData SnapshotDirty;

	rel = heap_open(relid, AccessShareLock);

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	scan = heap_beginscan_strat(rel,
								SnapshotAny,
								0,	/* nKeys */
								NULL,	/* key */
								true,	/* allow_strat */
								false	/* allow_sync */
		);

	InitDirtySnapshot(SnapshotDirty);
	MemSet(&stat, 0, sizeof(RangeStat));
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		/* Time to write a tuple */
		if (prev_blkno != InvalidBlockNumber &&
			prev_blkno != scan->rs_cblock &&
			(scan->rs_cblock % summary_size) == 0)
		{
			put_tuple(tupstore, tupdesc, stat);

			/* Re-initialize range summary */
			stat.rangeno++;
			stat.freespace = 0;
			stat.n_tuples = 0;
			stat.n_dead_tuples = 0;
			stat.n_all_visible = 0;
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		/* Collect stats about a block */
		if (prev_blkno != scan->rs_cblock)
		{
			Page	page = BufferGetPage(scan->rs_cbuf);
			Buffer	vmbuf = InvalidBuffer;
			int32	mapbits;

			/* Freespace */
			stat.freespace += PageGetFreeSpace(page);

			/* All-visible */
			mapbits = (int32) visibilitymap_get_status(rel, scan->rs_cblock, &vmbuf);
			if (vmbuf != InvalidBuffer)
				ReleaseBuffer(vmbuf);
			if ((mapbits & VISIBILITYMAP_ALL_VISIBLE) != 0)
				stat.n_all_visible++;
		}

		/* Collect stats about a tuple */
		if (!HeapTupleSatisfiesVisibility(tuple, &SnapshotDirty, scan->rs_cbuf))
			stat.n_dead_tuples++;
		stat.n_tuples++;

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		prev_blkno = scan->rs_cblock;
	}

	/* Write the last stats */
	put_tuple(tupstore, tupdesc, stat);

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/* Initialize functions */
void
_PG_walker_plugin_init(WALkerCallbacks *cb)
{
	cb->startup_cb = heatmap_startup;
	cb->heap_cb = heatmap_heap;
	cb->heap2_cb = heatmap_heap2;
	cb->xlog_cb = heatmap_xlog;
	cb->xact_cb = heatmap_xact;
	cb->smgr_cb = heatmap_smgr;
}

/*
 * Startup callback function.
 */
static void
heatmap_startup(void)
{
	HASHCTL	info;

	info.keysize = sizeof(RelFileNode);
	info.entrysize = sizeof(Garbagemap);

	GarbagemapHash = hash_create("Garbagemap hash",
							  1024,
							  &info,
							  HASH_ELEM | HASH_BLOBS);
}

/*
 * Process RM_HEAP_ID record.
 */
static void
heatmap_heap(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	RelFileNode node;
	BlockNumber blkno;

	/*
	 * Since XLOG_HEAP_INIT_PAGE flag could be set with another
	 * flag, we get rid of it.
	 */
	info &= ~XLOG_HEAP_INIT_PAGE;
	switch (info)
	{
		/* Make one page dirty */
		case XLOG_HEAP_INSERT:
		case XLOG_HEAP_DELETE:
		case XLOG_HEAP_HOT_UPDATE:
		case XLOG_HEAP_LOCK:
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			GarbagemapChangeTemprature(node, blkno, 1);
			break;

		/* Make two pages dirty */
		case XLOG_HEAP_UPDATE:
			{
				BlockNumber newblkno, oldblkno;

				XLogRecGetBlockTag(record, 0, &node, NULL, &newblkno);
				XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblkno);
				GarbagemapChangeTemprature(node, newblkno, 1);
				GarbagemapChangeTemprature(node, oldblkno, 1);
				break;
			}

		/* Ignore */
		case XLOG_HEAP_INPLACE:
		case XLOG_HEAP_CONFIRM:
		case XLOG_HEAP_INIT_PAGE:
			break;
		default:
			elog(ERROR, "unexpected RM_HEAP_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_HEAP2_ID record.
 */
static void
heatmap_heap2(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	RelFileNode node;
	BlockNumber blkno;

	switch (info)
	{
		/* Ignore */
		case XLOG_HEAP2_REWRITE:
		case XLOG_HEAP2_FREEZE_PAGE:
		case XLOG_HEAP2_CLEANUP_INFO:

		/* Make one page clean */
		case XLOG_HEAP2_CLEAN:
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			GarbagemapChangeTemprature(node, blkno, -1);

		/* Make one page clear */
		case XLOG_HEAP2_VISIBLE:
			XLogRecGetBlockTag(record, 1, &node, NULL, &blkno);
			GarbagemapCoolTemprature(node, blkno, MAP_RANGE_SIZE);
			break;

		/* Make one page dirty */
		case XLOG_HEAP2_MULTI_INSERT:
		case XLOG_HEAP2_LOCK_UPDATED:
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			GarbagemapChangeTemprature(node, blkno, 1);
			break;

		/* Ignore */
		case XLOG_HEAP2_NEW_CID:
			break;
		default:
			elog(ERROR, "unexpected RM_HEAP2_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_XLOG_ID record.
 */
static void
heatmap_xlog(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch(info)
	{
		/* Report summary of heatmap */
		case XLOG_CHECKPOINT_ONLINE:
			GarbagemapLogSummary();
			break;
		/* Ignore */
		case XLOG_CHECKPOINT_SHUTDOWN:
		case XLOG_NOOP:
		case XLOG_NEXTOID:
		case XLOG_SWITCH:
		case XLOG_BACKUP_END:
		case XLOG_PARAMETER_CHANGE:
		case XLOG_RESTORE_POINT:
		case XLOG_FPW_CHANGE:
		case XLOG_END_OF_RECOVERY:
		case XLOG_FPI_FOR_HINT:
		case XLOG_FPI:
			break;
		default:
			elog(ERROR, "unexpected RM_XLOG_ID record type: %u", info);
			break;
	}

}

/*
 * Process RM_SMGR_ID record.
 */
static void
heatmap_smgr(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		/* Ignore */
		case XLOG_SMGR_CREATE:
			break;
		case XLOG_SMGR_TRUNCATE:
			{
				xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
				GarbagemapClearTemprature(xlrec->rnode);
				break;
			}
		default:
			elog(ERROR, "unexpected RM_SMGR_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_XACT_ID record.
 */
static void
heatmap_xact(XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;

	switch (info)
	{
		/* Commit record could have relation truncation information */
		case XLOG_XACT_COMMIT:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
				xl_xact_parsed_commit parsed;

				ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);
				if (parsed.nrels > 0)
				{
					int i;
					for (i = 0; i < parsed.nrels; i++)
						GarbagemapClearTemprature(parsed.xnodes[i]);
				}
				break;
			}

		/* Ignore */
		case XLOG_XACT_PREPARE:
		case XLOG_XACT_ABORT:
		case XLOG_XACT_COMMIT_PREPARED:
		case XLOG_XACT_ABORT_PREPARED:
		case XLOG_XACT_ASSIGNMENT:
			break;
		default:
			elog(ERROR, "unexpected RM_XACT_ID record type: %u", info);
			break;

	}
}

/*
 * Record heap block changes of each relations. Increment one degree
 * of the temprature of given relation per thisfunction calls.
 */
static void
GarbagemapChangeTemprature(RelFileNode node, BlockNumber blkno, int delta_temp)
{
	int index;
	Garbagemap *hm;

	/* Skip if the given relation is biltins */
	if (node.relNode < FirstNormalObjectId)
		return;

	hm = GetGarbagemapEntry(node);
	index = TargetBlknoToIndex(blkno);

	/* XXX : Make sure we don't overflow */
	Assert(index < MAP_SIZE);

	/* Record the temprature of this range */
	hm->hmap[index] += delta_temp;
}

/*
 * Cool the temprature of one heap block changes of each relations. If
 * we decete the WALs that implies the given block doesn't has any
 * garbage we can cool the temprature of the block range by given
 * ratio.
 */
static void
GarbagemapCoolTemprature(RelFileNode node, BlockNumber blkno, int ratio)
{
	int index;
	Garbagemap *hm;

	/* Skip if the given relation is biltins */
	if (node.relNode < FirstNormalObjectId)
		return;

	hm = GetGarbagemapEntry(node);
	index = TargetBlknoToIndex(blkno);

	/* XXX : Make sure we don't overflow */
	Assert(index < MAP_SIZE);

	/* Clear temprature of this range */
	hm->hmap[index] /= ratio;
}

static void
GarbagemapClearTemprature(RelFileNode node)
{
	Garbagemap *hm;

	/* Skip if the given relation is biltins */
	if (node.relNode < FirstNormalObjectId)
		return;

	hm = GetGarbagemapEntry(node);
	memset(hm->hmap, 0, MAP_SIZE);
}

static Garbagemap*
GetGarbagemapEntry(RelFileNode node)
{
	Garbagemap *hm;
	bool	found;

	hm = (Garbagemap *) hash_search(GarbagemapHash,
								 (void *) &node,
								 HASH_ENTER, &found);

	/* Initialize */
	if (!found)
	{
		hm->node.spcNode = node.spcNode;
		hm->node.dbNode = node.dbNode;
		hm->node.relNode = node.relNode;
		memset(hm->hmap, 0, MAP_SIZE);
	}

	return hm;
}


/* Logging heatmap summary for debug purpose */
static void
GarbagemapLogSummary(void)
{
	HASH_SEQ_STATUS status;
	Garbagemap *entry;

	hash_seq_init(&status, GarbagemapHash);

	while ((entry = (Garbagemap *) hash_seq_search(&status)) != NULL)
	{
		char *dumped_map = GarbagemapDump(entry->hmap);
		RelFileNode node = entry->node;

		ereport(LOG,
				(errmsg("(%d,%d,%d) heatmap (%s)",
						node.spcNode, node.dbNode,node.relNode,
						dumped_map)));
	}
}

/* Return cstring-represented heatmap */
static char *
GarbagemapDump(int *heatmap)
{
	StringInfo buf = makeStringInfo();
	bool	isfirst = true;
	int		i, cur_temp;
	int		prev_temp = -1;
	int		prev_temp_count = 0;

	for (i = 0; i < MAP_SIZE; i++)
	{
		cur_temp = heatmap[i];

		if (cur_temp != prev_temp && prev_temp != -1)
		{
			/* Current temprature is different from previous one, log it */
			if (prev_temp_count > 1)
				appendStringInfo(buf, "%s%d:%d", (isfirst) ? "" : ", ",
								 prev_temp, prev_temp_count);
			else
				appendStringInfo(buf, "%s%d", (isfirst) ? "" : ", ",
								 prev_temp);

			isfirst = false;
			prev_temp_count = 1;
		}
		else
			prev_temp_count++;

		prev_temp = cur_temp;
	}

	if (prev_temp_count > 1)
		appendStringInfo(buf, "%s%d:%d", (isfirst) ? "" : ", ",
						 prev_temp, prev_temp_count);
	else
		appendStringInfo(buf, "%s%d", (isfirst) ? "" : ", ",
						 prev_temp);

	return buf->data;
}
