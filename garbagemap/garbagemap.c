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
#include <unistd.h>
#include <sys/stat.h>
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
#include "commands/vacuum.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/bufmgr.h"
#include "storage/standbydefs.h"
#include "storage/condition_variable.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/relfilenodemap.h"
#include "utils/rel.h"

#define	GARBAGE_SUMMARY_COLS 5

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(garbage_summary);
PG_FUNCTION_INFO_V1(pg_gmap_from_file);

/*
 * Garbagemap array size.
 * MapSize = TotalBlockSize/Range (Blocks/Range)
 * ------------------------
 * 8192  = 4 GB/range
 * 1 MB  = 32 MB/range (4096 blks/range)
 *
 * Note that actual map size is MapLength * 4byte so far.
 */
//#define MAP_SIZE (1024 * 1024)
#define MAP_LENGTH (1024 * 1024 * 4)	/* i.g, MAP_N_RANGES = 1024 */

/* Size of each range */
#define MAP_N_RANGES (MaxBlockNumber / MAP_LENGTH)

/* Convert target block number to slot of garbagemap */
#define TargetBlkToSlot(blkno) (blkno / MAP_N_RANGES)

/* Operation kind */
#define GM_KIND_INS 0
#define GM_KIND_DEL 1

#define GMAP_DIRNAME "pg_gmap"

#define GetGarbageMapFilePath(buf, node) \
	snprintf(buf, MAXPGPATH, "./%s/%d.%d.%d", GMAP_DIRNAME, node.spcNode, node.dbNode, node.relNode)

typedef int GarbageMapSlot;

/* Per relation garbage info */
typedef struct GarbageMapRel
{
	RelFileNode	node;	/* Key */
	int			map[MAP_LENGTH];
} GarbageMapRel;

/* Per transaction garbage info */
typedef struct GarbageMapTranEntry
{
	TransactionId	xid;	/* Key */
	List			*list;				/* List of GarbageMapTran */
} GarbageMapTranEntry;

/* Relation per transaction info */
typedef struct GarbageMapTran
{
	TransactionId	xid;
	RelFileNode		node;
	GarbageMapSlot	slot;
	int ins_value;
	int del_value;
} GarbageMapTran;

/* struct for summary output */
typedef struct RangStat
{
	int32	rangeno;
	uint32	freespace;
	uint32	n_tuples;
	uint32	n_dead_tuples;
	uint32	n_all_visible;
} RangeStat;

typedef struct GarbageMapShared
{
	pid_t	walker_pid;
	ConditionVariable walker_cv;
} GarbageMapShared;

/* GUC parameter */
static int32 summary_size;
static int min_range_vacuum_size;
static int range_vacuum_percent;

/* hash table for walker */
static HTAB *GarbageMapRelHash;
static HTAB *GarbageMapTranHash;

/* hash table for local backend */
static HTAB *GarbageMapRelLocalHash;

/* Shared memory space */
static GarbageMapShared *GMShared = NULL;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static int vacuum_counted = 0;	/* track of number of vacuumed pages */

/* Plugin handler function */
extern void _PG_walker_plugin_init(WALkerCallbacks *cb);

void		_PG_init(void);

/* Hook function */
static VacuumWorkItem *garbagemap_workitem_hook(Relation onerel,
												VacuumWorkItem *workitem,
												int options);

/* Callback functions */
static void garbagemap_startup(void);
static void garbagemap_heap(XLogReaderState *record);
static void garbagemap_heap2(XLogReaderState *record);
static void garbagemap_xlog(XLogReaderState *record);
static void garbagemap_xact(XLogReaderState *record);
static void garbagemap_smgr(XLogReaderState *record);
static void garbagemap_standby(XLogReaderState *record);

/* Garbagemap processing functions */
/*
static void GarbagemapChangeTemprature(RelFileNode node, BlockNumber blkno,
									int delta_temp);
static void GarbagemapCoolTemprature(RelFileNode node, BlockNumber blkno,
								  int ratio);
static void GarbagemapClearTemprature(RelFileNode node);
static Garbagemap *GetGarbagemapEntry(RelFileNode node);
*/
static void GMTranQueueCountInsert(RelFileNode node, TransactionId xid, BlockNumber blk,
								   int count);
static void GMTranQueueCountDelete(RelFileNode node, TransactionId xid, BlockNumber blk,
								   int count);
static void GMTranQueueCount_common(RelFileNode node, TransactionId xid, BlockNumber blk,
									int count, int kind);
static void GMRelCountVacuum(RelFileNode node, BlockNumber blk, int count);
static void GMRelCountSet(RelFileNode node, BlockNumber startblk, int count);
static void GMRelGatherTrans(TransactionId xid, RelFileNode *ignore_nodes, int nrels,
							 bool isCommit);
static void GMRelWriteDumpFile(GarbageMapRel *gmaprel, bool start_tx);
static void GMRelDumpALl(void);
static bool GMRelReadDumpFile(RelFileNode node, GarbageMapRel **gmaprel);
static bool GMRelGetLatestDataFromDumpFile(RelFileNode node, GarbageMapRel **gmaprel);
static void GMShmemStartup(void);

static void put_tuple(Tuplestorestate *tupstore, TupleDesc tupdesc, RangeStat stat);

static GarbageMapTran *get_gmaptran(RelFileNode node, TransactionId xid, BlockNumber blk);
static GarbageMapTran *get_new_gmaptran(void);
static int gmaptran_compare(const void *a, const void *b);
static inline void gmaprel_get_range(GarbageMapSlot slot, BlockNumber *start, BlockNumber *end);

/* Methods to calculate ranges */
static BlockNumber *gmap_highest_one(Relation onerel, GarbageMapRel *gmaprel);
static BlockNumber *gmap_highest_n(Relation onerel, GarbageMapRel *gmaprel);
static BlockNumber *gmap_highest_n_percent(Relation onerel, GarbageMapRel *gmaprel);

/* Debug purpose */
static void GMRelSummary(void);
static char *GarbagemapDump(int *garbagemap);

static void gm_shmem_on_exit_callback(int code, Datum arg);

void
_PG_init(void)
{
	DefineCustomIntVariable("garbagemap.summary_size",
							"The number of blocks for summarizing",
							NULL,
							&summary_size,
							1024, /* 1024 blocks = 8 MB */
							1,
							INT_MAX,
							PGC_USERSET,
							GUC_UNIT_BLOCKS,
							NULL, NULL, NULL);
	DefineCustomIntVariable("garbagemap.range_vacuum_percent",
							"The ratio of pages being vacuumed",
							NULL,
							&range_vacuum_percent,
							30,
							0,
							100,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("garbagemap.min_range_vacuum_size",
							"Sets the minimum amount of table data for range vacuum",
							NULL,
							&min_range_vacuum_size,
							1024 * 1024, /* 1GB */
							0,
							INT_MAX,
							PGC_USERSET,
							GUC_UNIT_BLOCKS,
							NULL, NULL, NULL);

	RequestAddinShmemSpace(sizeof(GarbageMapShared));

	vacuum_get_workitem_hook = garbagemap_workitem_hook;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = GMShmemStartup;
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

Datum
pg_gmap_from_file(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	GarbageMapRel *gmaprel = palloc(sizeof(GarbageMapRel));
	char filepath[MAXPGPATH];
	RelFileNode node;
	Relation rel;

	rel = heap_open(relid, AccessShareLock);
	node = rel->rd_node;

	GetGarbageMapFilePath(filepath, node);

	GMRelReadDumpFile(node, &gmaprel);

	elog(NOTICE, "\"%s\" [%s]", RelationGetRelationName(rel),
		 GarbagemapDump(gmaprel->map));

	heap_close(rel, AccessShareLock);
	pfree(gmaprel);

	PG_RETURN_NULL();
}

/* Initialize functions */
void
_PG_walker_plugin_init(WALkerCallbacks *cb)
{
	cb->startup_cb = garbagemap_startup;
	cb->heap_cb = garbagemap_heap;
	cb->heap2_cb = garbagemap_heap2;
	cb->xlog_cb = garbagemap_xlog;
	cb->xact_cb = garbagemap_xact;
	cb->smgr_cb = garbagemap_smgr;
	cb->standby_cb = garbagemap_standby;
}

static void
GMShmemStartup(void)
{
	bool found;

	GMShared = ShmemInitStruct("Gargabemap Shared",
							   sizeof(GarbageMapShared),
							   &found);
	ConditionVariableInit(&(GMShared->walker_cv));
}

static void
garbagemap_sigusr1(SIGNAL_ARGS)
{
	GMRelDumpALl();
	ConditionVariableBroadcast(&(GMShared->walker_cv));
}

/*
 * Startup callback function.
 */
static void
garbagemap_startup(void)
{
	MemoryContext ctx;
	HASHCTL	info;
	DIR		*dir;
	struct dirent *de;

	/* Set my PID */
	GMShared->walker_pid = MyProcPid;

	/* Set signal handler */
	pqsignal(SIGUSR1, garbagemap_sigusr1);

	ctx = MemoryContextSwitchTo(TopMemoryContext);

	info.keysize = sizeof(RelFileNode);
	info.entrysize = sizeof(GarbageMapRel);

	GarbageMapRelHash = hash_create("Garbagemap hash",
									1024,
									&info,
									HASH_ELEM | HASH_BLOBS);

	info.keysize = sizeof(TransactionId);
	info.entrysize = sizeof(GarbageMapTran);
	GarbageMapTranHash = hash_create("Garbagemap Transaction hash",
									 1024,
									 &info,
									 HASH_ELEM | HASH_BLOBS);
	MemoryContextSwitchTo(ctx);

	/* Read all dumped files */
	dir = AllocateDir("pg_gmap");
	while ((de = ReadDir(dir, "pg_gmap")) != NULL)
	{
		RelFileNode n;
		GarbageMapRel *gmaprel;

		/* Skip special stuff */
		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		sscanf(de->d_name, "%d.%d.%d", &n.spcNode, &n.dbNode, &n.relNode);
		gmaprel = hash_search(GarbageMapRelHash, (void *) &(n),
							  HASH_ENTER, NULL);

		ereport(LOG, (errmsg("restored %u %u %u hash %u",
							 n.relNode, n.dbNode, n.spcNode,
							 get_hash_value(GarbageMapRelHash, (void *) &(n))
						  )));

		GMRelReadDumpFile(n, &gmaprel);
	}
}

/*
 * Process RM_HEAP_ID record.
 */
static void
garbagemap_heap(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	RelFileNode node;
	BlockNumber blkno;
	TransactionId xid;

	/*
	 * Since XLOG_HEAP_INIT_PAGE flag could be set with another
	 * flag, we get rid of it.
	 */
	info &= ~XLOG_HEAP_INIT_PAGE;
	XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
	xid =  XLogRecGetXid(record);
	switch (info)
	{
		/* Make one page dirty */
		case XLOG_HEAP_INSERT:
			GMTranQueueCountInsert(node, xid, blkno, 1);
			break;
		case XLOG_HEAP_DELETE:
			GMTranQueueCountDelete(node, xid, blkno, 1);
			break;
		case XLOG_HEAP_HOT_UPDATE:
			/* HOT update doesn't make garbage? */
			GMTranQueueCountDelete(node, xid, blkno, 1);
			GMTranQueueCountInsert(node, xid, blkno, 1);
			break;
		case XLOG_HEAP_UPDATE:
			{
				BlockNumber newblkno, oldblkno;

				XLogRecGetBlockTag(record, 0, &node, NULL, &newblkno);
				GMTranQueueCountInsert(node, xid, newblkno, 1);

				if (XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblkno))
					GMTranQueueCountDelete(node, xid, oldblkno, 1);
				else
					GMTranQueueCountDelete(node, xid, newblkno, 1);

				break;
			}
		/* Ignore */
		case XLOG_HEAP_TRUNCATE:
		case XLOG_HEAP_LOCK:
		case XLOG_HEAP_INPLACE:
		case XLOG_HEAP_CONFIRM:
		case XLOG_HEAP_INIT_PAGE:
			break;
		default:
			elog(WARNING, "unexpected RM_HEAP_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_HEAP2_ID record.
 */
static void
garbagemap_heap2(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & XLOG_HEAP_OPMASK;
	RelFileNode node;
	BlockNumber blkno;
	TransactionId xid;

	XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
	xid =  XLogRecGetXid(record);
	switch (info)
	{
		/* Ignore */
		case XLOG_HEAP2_REWRITE:
		case XLOG_HEAP2_FREEZE_PAGE:
			break;
		case XLOG_HEAP2_CLEANUP_INFO:
		{
			xl_heap_cleanup_info *xlrec;

			xlrec = (xl_heap_cleanup_info *) XLogRecGetData(record);
			if (BlockNumberIsValid(xlrec->startblk))
				GMRelCountSet(xlrec->node, xlrec->startblk, xlrec->new_dead_tuples);
		}
		break;

		/* Make one page clean */
		case XLOG_HEAP2_CLEAN:
		{
			xl_heap_clean *xlrec;
			OffsetNumber *end;
			OffsetNumber *redirected;
			OffsetNumber *nowdead;
			OffsetNumber *nowunused;
			int         nredirected;
			int         ndead;
			int         nunused;
			Size        datalen;

			xlrec = (xl_heap_clean *) XLogRecGetData(record);
			redirected = (OffsetNumber *) XLogRecGetBlockData(record, 0, &datalen);

			nredirected = xlrec->nredirected;
			ndead = xlrec->ndead;
			end = (OffsetNumber *) ((char *) redirected + datalen);
			nowdead = redirected + (nredirected * 2);
			nowunused = nowdead + ndead;
			nunused = Max(end - nowunused, 0);

//			elog(WARNING, "rel %d, blk %u, ndead %d, nunused %d, nredirected %d",
//				 node.relNode, blkno, ndead, nunused, nredirected);
//			GMRelCountVacuum(node, blkno, nunused + nredirected);
			break;
		}
		case XLOG_HEAP2_VISIBLE:
//			XLogRecGetBlockTag(record, 1, &node, NULL, &blkno);
//			GarbagemapCoolTemprature(node, blkno, MAP_N_RANGES);
			break;

		/* Make one page dirty */
		case XLOG_HEAP2_MULTI_INSERT:
		{
			xl_heap_multi_insert *xlrec;

			xlrec = (xl_heap_multi_insert *) XLogRecGetData(record);
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			GMTranQueueCountInsert(node, xid, blkno, xlrec->ntuples);
			break;
		}

		/* Ignore */
		case XLOG_HEAP2_LOCK_UPDATED:
		case XLOG_HEAP2_NEW_CID:
			break;
		default:
			elog(WARNING, "unexpected RM_HEAP2_ID record type: %X", info);
			break;
	}
}

/*
 * Process RM_XLOG_ID record.
 */
static void
garbagemap_xlog(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch(info)
	{
		/* Report summary of garbagemap */
		case XLOG_CHECKPOINT_ONLINE:
			GMRelSummary();
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
			elog(WARNING, "unexpected RM_XLOG_ID record type: %u", info);
			break;
	}

}

/*
 * Process RM_SMGR_ID record.
 */
static void
garbagemap_smgr(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		/* Ignore */
		case XLOG_SMGR_CREATE:
			break;
		case XLOG_SMGR_TRUNCATE:
			{
//				xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(record);
//				GMTranQueueCountTrancate(xlrec->rnode, xid);
				break;
			}
		default:
			elog(WARNING, "unexpected RM_SMGR_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_STANDBY_ID record.
 */
static void
garbagemap_standby(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_INVALIDATIONS:
		case XLOG_RUNNING_XACTS:
			{
//				GMRelDumpALl();
				break;
			}
		/* Ignore */
		case XLOG_STANDBY_LOCK:
		break;
		default:
			elog(WARNING, "unexpected RM_SMGR_ID record type: %u", info);
			break;
	}
}

/*
 * Process RM_XACT_ID record.
 */
static void
garbagemap_xact(XLogReaderState *record)
{
	uint8       info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	TransactionId xid;

	xid =  XLogRecGetXid(record);
	switch (info)
	{
		/* Commit record could have relation truncation information */
		case XLOG_XACT_COMMIT:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(record);
				xl_xact_parsed_commit parsed;

				ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);
				GMRelGatherTrans(xid, parsed.xnodes, parsed.nrels, true);
				break;
			}
		case XLOG_XACT_ABORT:
			{
				xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(record);
				xl_xact_parsed_abort parsed;

				ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed);
				GMRelGatherTrans(xid, parsed.xnodes, parsed.nrels, false);
				break;
			}

		/* Ignore */
		case XLOG_XACT_PREPARE:
		case XLOG_XACT_COMMIT_PREPARED:
		case XLOG_XACT_ABORT_PREPARED:
		case XLOG_XACT_ASSIGNMENT:
			break;
		default:
			elog(WARNING, "unexpected RM_XACT_ID record type: %u", info);
			break;

	}
}

/* Return cstring-represented garbagemap */
static char *
GarbagemapDump(int *garbagemap)
{
	StringInfo buf = makeStringInfo();
	bool	isfirst = true;
	int		i, cur_temp;
	int		prev_temp = -1;
	int		prev_temp_count = 0;

	for (i = 0; i < MAP_LENGTH; i++)
	{
		cur_temp = garbagemap[i];

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

static void
GMTranQueueCountInsert(RelFileNode node, TransactionId xid, BlockNumber blk,
					  int count)
{
	GMTranQueueCount_common(node, xid, blk, count, GM_KIND_INS);
}

static void
GMTranQueueCountDelete(RelFileNode node, TransactionId xid, BlockNumber blk,
					  int count)
{
	GMTranQueueCount_common(node, xid, blk, count, GM_KIND_DEL);
}

/*
 * Queue the change count to the 'xid' transaction.
 */
static void
GMTranQueueCount_common(RelFileNode node, TransactionId xid, BlockNumber blk,
						int count, int kind)
{
	GarbageMapTran	*gmaptran;

	if (node.relNode < FirstNormalObjectId)
		return;

	/* Get tran entry */
	gmaptran = get_gmaptran(node, xid, blk);

	/* Do something */
	switch (kind)
	{
		case GM_KIND_INS:
			gmaptran->ins_value += count;
			break;
		case GM_KIND_DEL:
			gmaptran->del_value += count;
			break;
	}
/*
	elog(WARNING, "REC xid %u, node %u, slot %d, blk %d, cnd %d, val (%d,%d) kind %s",
		 xid, node.relNode, TargetBlkToSlot(blk), blk,
		 count,
		 gmaptran->ins_value,
		 gmaptran->del_value,
		 (kind == GM_KIND_INS) ? "INS" : "DEL");
*/
}

static GarbageMapTran *
get_gmaptran(RelFileNode node, TransactionId xid, BlockNumber blk)
{
	static GarbageMapTran *gmaptran_cache = NULL;
	GarbageMapTranEntry	*gmaptran_entry;
	GarbageMapTran	*cur;
	GarbageMapSlot	slot;
	bool			found;
	ListCell		*cell;

	if (gmaptran_cache &&
		gmaptran_cache->xid == xid &&
		RelFileNodeEquals(gmaptran_cache->node, node) &&
		gmaptran_cache->slot == TargetBlkToSlot(blk))
		return gmaptran_cache;

	gmaptran_entry = (GarbageMapTranEntry *) hash_search(GarbageMapTranHash,
														 (void *) &xid,
														 HASH_ENTER, &found);
	slot = TargetBlkToSlot(blk);

	/* Initialize */
	if (!found)
		gmaptran_entry->list = NIL;

	/* Find the entry {node,slot} in chain */
	found = false;
	foreach(cell, gmaptran_entry->list)
	{
		cur = lfirst(cell);

		if (RelFileNodeEquals(cur->node, node) && cur->slot == slot)
		{
			found = true;
			break;
		}
	}

	/* If not found in the chain, create new entry */
	if (!found)
	{
		GarbageMapTran *new;

		/* Get new one, initialize */
		new = get_new_gmaptran();
		new->node = node;
		new->slot = slot;
		new->xid = xid;

		gmaptran_entry->list = lappend(gmaptran_entry->list, new);
		cur = new;
	}

	/* cache entry */
	gmaptran_cache = cur;

	return cur;
}

static GarbageMapTran *
get_new_gmaptran(void)
{
	GarbageMapTran *gmaptran;
	MemoryContext ctx;

	ctx = MemoryContextSwitchTo(TopMemoryContext);

	gmaptran = palloc(sizeof(GarbageMapTran));
	gmaptran->slot = 0;
	gmaptran->ins_value = gmaptran->del_value = 0;

	MemoryContextSwitchTo(ctx);

	return gmaptran;
}

/*
 * Gather garbage info of all transactions associated with the 'xid'. This
 * is used when replaying COMMIT record or ABORT record. Having ignore_rels
 * != NULL (therefore nrels > 0) means that a TRUNCATE command has been
 * performed within this transaction. In this case, we ignore the per-
 * transaction change related to the relation that is listed in 'ignore_rels',
 * and then cleanup them the end of this routine.
 */
static void
GMRelGatherTrans(TransactionId xid, RelFileNode *ignore_rels, int nrels,
				 bool isCommit)
{
	GarbageMapTranEntry	*gmaptran_entry;
	bool			found;
	ListCell		*cell;
	GarbageMapRel	*cache;
	List			*dump_rels = NIL;
	int	i;

	/* Get chain-ed transaction info */
	gmaptran_entry = (GarbageMapTranEntry *) hash_search(GarbageMapTranHash,
														 (void *) &xid,
														 HASH_ENTER, &found);
	if (!found)
	{
		goto cleanup;
		return;
	}

	/* Sort by RelFileNode*/
	gmaptran_entry->list = list_qsort(gmaptran_entry->list, gmaptran_compare);

//	if (nrels > 0)
//		elog(WARNING, "GATHER ignore %d rels: %d", nrels, ignore_rels[0].relNode);

	/* Iterate over chain */
	cache = NULL;
	foreach(cell, gmaptran_entry->list)
	{
		GarbageMapTran	*cur = lfirst(cell);
		GarbageMapRel	*gmaprel;
		bool			found;
		bool			skip = false;

		/*
		 * If this transaction did TRANCATE, we ignore the trans info
		 * related to the relation which is listed in ignore_rels.
		 */
		for (i = 0; i < nrels; i++)
		{
			if (RelFileNodeEquals(cur->node, ignore_rels[i]))
			{
				skip = true;
				break;
			}
		}
		if (skip)
			continue;

		/*
		 * We gather transaction info into garbage map of relation,
		 * get GarbageMapRel.
		 */
		if (!cache ||
			!RelFileNodeEquals(cache->node, cur->node))
		{
			/* Get relation garbage map */
			gmaprel = hash_search(GarbageMapRelHash,
								  (void *) &(cur->node),
								  HASH_ENTER, &found);

			if (!found)
			{
				elog(WARNING, "created new one! %u %u %u hash %u",
					 cur->node.relNode, cur->node.dbNode, cur->node.spcNode,
					 get_hash_value(GarbageMapRelHash, (void *) &(cur->node)));
				elog(WARNING, "memset 1");
				MemSet(gmaprel->map, 0, sizeof(gmaprel->map));
			}

			/* Remember rels to be dumped */
			dump_rels = lappend(dump_rels, gmaprel);
		}
		else
			gmaprel = cache;	/* same as prev relnode */

		if (isCommit)
			gmaprel->map[cur->slot] += cur->del_value;
		else
			gmaprel->map[cur->slot] += cur->ins_value;

		/* Keep sane value */
//		if (gmaprel->map[cur->slot] < 0)
//			gmaprel->map[cur->slot] = 0;

		/* cache the this rel */
		cache = gmaprel;

		/*
		elog(WARNING, "GATGER xid %u, node %u, map[%d] = %d, did +%d",
			 cur->xid,
			 gmaprel->node.relNode,
			 cur->slot, gmaprel->map[cur->slot],
			 isCommit ? cur->del_value : cur->ins_value);
		*/
	}


	/* Dump the all updated rels */
	/*
	foreach(cell, dump_rels)
	{
//		GarbageMapRel *gmaprel = lfirst(cell);

		GMRelWriteDumpFile((GarbageMapRel *) lfirst(cell), true);
//		elog(WARNING, "walker : rel:%d [%s]", gmaprel->node.relNode,
//			 GarbagemapDump(gmaprel->map));
	}
	*/

	/* Cleanup all transaction-garbage info */
	list_free_deep(gmaptran_entry->list);
	/* Cleanup transaction info */
	hash_search(GarbageMapTranHash, (void *) &xid,
				HASH_REMOVE, NULL);
cleanup:
	/* Cleanup truncated rels */
	for (i = 0; i < nrels; i++)
	{
		char filepath[MAXPGPATH];

		hash_search(GarbageMapRelHash, (void *) &ignore_rels[i],
					HASH_REMOVE, NULL);
		GetGarbageMapFilePath(filepath, ignore_rels[i]);
		unlink(filepath);
//		elog(WARNING, "unlinked %s", filepath);
	}
}

static void
GMRelCountSet(RelFileNode node, BlockNumber startblk, int count)
{
	GarbageMapRel	*gmaprel;
	GarbageMapSlot	slot;
	bool			found;

	if (node.relNode < FirstNormalObjectId)
		return;

	gmaprel = hash_search(GarbageMapRelHash,
						  (void *) &node,
						  HASH_ENTER, &found);
	if (!found)
	{
//		elog(WARNING, "memset 2");
		MemSet(gmaprel->map, 0, sizeof(gmaprel->map));
	}

	slot = TargetBlkToSlot(startblk);
	gmaprel->map[slot] = count;

	/*
	elog(WARNING, "COUNT SET map[%d] = %d, startblk %u [%s], map %p",
		 slot,
		 count,
		 startblk,
		 GarbagemapDump(gmaprel->map),
p		 gmaprel->map);
	*/

	/* Dump file */
	GMRelWriteDumpFile(gmaprel, false);
}

/*
 * Vacuum is not transactional. Modify the garbagemap of relation
 * directly.
 */
static void
GMRelCountVacuum(RelFileNode node, BlockNumber blk, int count)
{
	GarbageMapRel	*gmaprel;
	GarbageMapSlot	slot;
	bool			found;

	if (node.relNode < FirstNormalObjectId)
		return;

	gmaprel = hash_search(GarbageMapRelHash,
						  (void *) &node,
						  HASH_ENTER, &found);
	if (!found)
	{
//		elog(WARNING, "memset 3");
		MemSet(gmaprel->map, 0, sizeof(gmaprel->map));
	}

	slot = TargetBlkToSlot(blk);
	gmaprel->map[slot] -= count;

	if (gmaprel->map[slot] < 0)
		gmaprel->map[slot] = 0;

	vacuum_counted++;

	/* Once we replayed blocks for a range, dump/notify backend */
	if (vacuum_counted >= MAP_N_RANGES)
	{
		//GMRelWriteDumpFile(gmaprel, true);
		vacuum_counted = 0;
	}

//	elog(WARNING, "VACUUM node %u, map[%d] = %d, did -%d",
//		 gmaprel->node.relNode, slot, gmaprel->map[slot], count);
}

static int
gmaptran_compare(const void *a, const void *b)
{
	GarbageMapTran *ta = lfirst(*(ListCell **) a);
	GarbageMapTran *tb = lfirst(*(ListCell **) b);

	if (RelFileNodeEquals(ta->node, tb->node))
		return 0;

	if (ta->node.relNode - tb->node.relNode > 0)
		return 1;
	else
		return -1;
}

static
void GMRelSummary(void)
{
	HASH_SEQ_STATUS status;
	GarbageMapRel *entry;

	hash_seq_init(&status, GarbageMapRelHash);

	StartTransactionCommand();
	while ((entry = (GarbageMapRel *) hash_seq_search(&status)) != NULL)
	{
		char *dumped_map = GarbagemapDump(entry->map);
		RelFileNode node = entry->node;

		if (get_rel_name(RelidByRelfilenode(node.spcNode, node.relNode)) == NULL)
		{
			char filepath[MAXPGPATH];
			GetGarbageMapFilePath(filepath, node);
			unlink(filepath);
			continue;
		}

		ereport(LOG,
				(errmsg("\"%s\" %u %p [%s] MAP_SIZE %d : N_RANGES %d",
						get_rel_name(RelidByRelfilenode(node.spcNode, node.relNode)),
						node.relNode,
						entry->map,
						dumped_map,
						MAP_LENGTH, MAP_N_RANGES)));
		GMRelWriteDumpFile(entry, false);
	}
	CommitTransactionCommand();
}

static void
GMRelDumpALl(void)
{
	HASH_SEQ_STATUS status;
	GarbageMapRel *entry;

	hash_seq_init(&status, GarbageMapRelHash);

	while ((entry = (GarbageMapRel *) hash_seq_search(&status)) != NULL)
		GMRelWriteDumpFile(entry, false);
}

static void
GMRelWriteDumpFile(GarbageMapRel *gmaprel, bool start_tx)
{
	char filepath[MAXPGPATH];
	FILE *fp;

	GetGarbageMapFilePath(filepath, gmaprel->node);

	if (start_tx)
		StartTransactionCommand();
	fp = AllocateFile(filepath, PG_BINARY_W);
	if (!fp)
	{
		elog(WARNING, "could not open dump file \"%s\" for rel %d",
			 filepath, gmaprel->node.relNode);
		return;
	}

	/* Write garbage map */
	fwrite(gmaprel, sizeof(GarbageMapRel), 1, fp);
	FreeFile(fp);

	if (start_tx)
		CommitTransactionCommand();
//	elog(LOG, "Dumped \"%s\", len %lu", filepath, sizeof(GarbageMapRel));
}

static bool
GMRelGetLatestDataFromDumpFile(RelFileNode node, GarbageMapRel **gmaprel)
{
	char filepath[MAXPGPATH];
	struct stat statbuf;
	pg_time_t	request_time;

	GetGarbageMapFilePath(filepath, node);

	request_time = time(NULL);

	stat(filepath, &statbuf);
	elog(NOTICE, "before... request time %ld, mtime %ld", request_time, statbuf.st_mtime);

	/* Ask walker process to update dump file by signal */
	kill(GMShared->walker_pid, SIGUSR1);

	for (;;)
	{
		if (QueryCancelPending)
		{
			ereport(WARNING,
					(errmsg("canceling wait for updating garbagemap file")));
			QueryCancelPending = false;
			break;
		}

		if (stat(filepath, &statbuf) == 0)
		{
			if (request_time <= statbuf.st_mtime)
				break;
		}

		ConditionVariableSleep(&(GMShared->walker_cv), PG_WAIT_EXTENSION);
	}

	ConditionVariableCancelSleep();

	/* Ok, dump file has been updated. Read it */
	if (!GMRelReadDumpFile(node, gmaprel))
		return false;

	return true;
}

static bool
GMRelReadDumpFile(RelFileNode node, GarbageMapRel **gmaprel)
{
	char filepath[MAXPGPATH];
	FILE *fp;

	GetGarbageMapFilePath(filepath, node);

	fp = AllocateFile(filepath, PG_BINARY_R);

	if (!fp)
	{
		elog(WARNING, "could not read dumped file \"%s\" for rel %d",
			 filepath, node.relNode);
		return false;
	}

	fread(*gmaprel, sizeof(GarbageMapRel), 1, fp);
	FreeFile(fp);
//	elog(WARNING, "Read \"%s\" : rel %d", filepath, (*gmaprel)->node.relNode);
	return true;
}

static VacuumWorkItem *
garbagemap_workitem_hook(Relation onerel, VacuumWorkItem *workitem, int options)
{
	GarbageMapRel	*gmaprel;
	BlockNumber		*vacrange;
	bool			found;
	int				i;

	/* Don't support system catalogs */
	if (RelationGetRelid(onerel) < FirstNormalObjectId)
		return workitem;

	if (RelationGetNumberOfBlocks(onerel) < min_range_vacuum_size)
		return workitem;

	if (!GarbageMapRelLocalHash)
	{
		HASHCTL info;
		info.keysize = sizeof(RelFileNode);
		info.entrysize = sizeof(GarbageMapRel);

		GarbageMapRelLocalHash = hash_create("Garbage Local Map",
											 1024,
											 &info,
											 HASH_ELEM | HASH_BLOBS);
	}

	gmaprel = hash_search(GarbageMapRelLocalHash,
						  (void *) &(onerel->rd_node),
						  HASH_ENTER, &found);
//	if (!found)
	if (true)	/* Always read from file!! */
	{
		if (!GMRelGetLatestDataFromDumpFile(onerel->rd_node, &gmaprel))
//		if (!GMRelReadDumpFile(onerel->rd_node, &gmaprel))
		{
			hash_search(GarbageMapRelLocalHash,
						(void *) &(onerel->rd_node),
						HASH_REMOVE, NULL);
			elog(WARNING, "could not find :(");
			return workitem;
		}
	}

	/*
	 * Okay, we got the garbage map for this relation here.
	 * We consider more effecient way to vacuum this relation.
	 */

	/* Dump for debugging */
	elog(INFO, "backend : \"%s\" [%s]", RelationGetRelationName(onerel),
		 GarbagemapDump(gmaprel->map));

	/* Choose one method */
	//vacrange = gmap_highest_one(onerel, gmaprel);
	//vacrange = gmap_highest_n(onerel, gmaprel);
	vacrange = gmap_highest_n_percent(onerel, gmaprel);
	//:

	/* Set vacuum range for returning */
	workitem->wi_vacrange = vacrange;

	elog(INFO, "---- RESULT RANGE ----");
	for (i = 0; workitem->wi_vacrange[i] != InvalidBlockNumber; i = i + 2)
	{
		BlockNumber start, end;
		start = workitem->wi_vacrange[i];
		end = workitem->wi_vacrange[i + 1];
		elog(INFO, "range[%d] %d - %d (%d blks)", start / MAP_N_RANGES,
			 start, end, end - start + 1);
	}
	elog(INFO, "----------------------");

	return workitem;
}

static inline void
gmaprel_get_range(GarbageMapSlot slot, BlockNumber *start, BlockNumber *end)
{
	*start = MAP_N_RANGES * slot;
	*end = MAP_N_RANGES * (slot + 1) -1;
}

/*
 * Method 1.
 * Choose one range haivng most garbages.
 */
static BlockNumber *
gmap_highest_one(Relation onerel, GarbageMapRel *gmaprel)
{
	int			max = 0, max_slot = 0;
	BlockNumber	start, end;
	int 		i;
	BlockNumber	*result = palloc(sizeof(BlockNumber) * (2 + 1));

	elog(INFO, "----- Select one range having most garbage -----");

	/* Find highest one range */
	for (i = 0; i < MAP_N_RANGES; i++)
	{
		if (max < gmaprel->map[i])
		{
			max_slot = i;
			max = gmaprel->map[i];
			elog(INFO, "map[%d] = %d", i, gmaprel->map[i]);
		}
	}

	/* Get range(start/end) by slot number */
	gmaprel_get_range(max_slot, &start, &end);

	/* Keep valid value */
	start = Max(start, 0);
	end = Min(end, RelationGetNumberOfBlocks(onerel) - 1);

	result[0] = start;
	result[1] = end;
	result[2] = InvalidBlockNumber;
	elog(INFO, "max slot %d val %d", max_slot, max);
	elog(INFO, "-------------------------------------------------");

	return result;
}


/*
 * Method 2.
 * Choose several range haivng most garbages.
 */
typedef struct ValueWithIndex
{
	int value;
	int idx;
} ValueWithIndex;
static int
vwi_compare(const void *a, const void *b)
{
	ValueWithIndex *v1 = (ValueWithIndex *) a;
	ValueWithIndex *v2 = (ValueWithIndex *) b;

	if (v1->value == v2->value)
	{
		if (v1->idx - v2->idx > 0)
			return -1;
		else
			return 1;
	}

	if (v1->value - v2->value > 0)
		return -1;
	else
		return 1;
}
static int
vwi_compare_index(const void *a, const void *b)
{
	ValueWithIndex *v1 = (ValueWithIndex *) a;
	ValueWithIndex *v2 = (ValueWithIndex *) b;

	if (v1->idx == v2->idx)
		return 0;
	if (v1->idx > v2->idx)
		return 1;
	else
		return -1;
}

static BlockNumber *
gmap_highest_n_percent(Relation onerel, GarbageMapRel *gmaprel)
{
	int i, cnt, n_choosen;
	ValueWithIndex vwi[MAP_N_RANGES];
	ValueWithIndex vwi_sorted[MAP_N_RANGES];
	BlockNumber *result = palloc(sizeof(BlockNumber) * (MAP_N_RANGES * 2 + 1));
	int total_value = 0;
	int value_limit;

	elog(INFO, "----- Select higher %d percent ranges -----", range_vacuum_percent);

	/* Construct mapping {value, idx} */
	for (i = 0; i < MAP_N_RANGES; i++)
	{
		vwi[i].value = gmaprel->map[i];
		vwi[i].idx = i;
		total_value += gmaprel->map[i];
	}

	/* Sort it by desc order of value and idx */
	qsort(vwi, MAP_N_RANGES, sizeof(ValueWithIndex), vwi_compare);

	value_limit = total_value * ((float)range_vacuum_percent / 100);

	n_choosen = 0;
	total_value = 0;
	for (i = 0; i < MAP_N_RANGES; i++)
	{
		/* Don't count invalid value */
		if (vwi[i].value <= 0)
			break;

		/* Reached limit? */
		if (total_value >= value_limit)
			break;

		vwi_sorted[n_choosen++] = vwi[i];
		total_value += vwi[i].value;
	}

	/* Sort by index in asc order */
	qsort(vwi_sorted, n_choosen, sizeof(ValueWithIndex), vwi_compare_index);

	cnt = 0;
	for (i = 0; i < n_choosen; i ++)
	{
		BlockNumber start, end;

		/* Get start/end block number in the range */
		gmaprel_get_range(vwi_sorted[i].idx, &start, &end);
		start = Max(start, 0);
		end = Min(end, RelationGetNumberOfBlocks(onerel) - 1);

		result[cnt++] = start;
		result[cnt++] = end;
		elog(INFO, "map[%d] = %d, start %u, end %u", vwi_sorted[i].idx, vwi_sorted[i].value,
			 start, end);
	}

	elog(INFO, "---------- CHOOSE %d ranges-----------------", cnt / 2);

	result[cnt] = InvalidBlockNumber;

	return result;
}

static BlockNumber *
gmap_highest_n(Relation onerel, GarbageMapRel *gmaprel)
{
#define N_CHOOSE 10 /* 10 ranges = 320MB */
	int i, cnt;
	ValueWithIndex vwi[MAP_N_RANGES];
	BlockNumber *result = palloc(sizeof(BlockNumber) * (N_CHOOSE * 2 + 1));

	elog(INFO, "----- Select highest %d ranges -----", N_CHOOSE);

	/* Construct mapping {value, idx} */
	for (i = 0; i < MAP_N_RANGES; i++)
	{
		vwi[i].value = gmaprel->map[i];
		vwi[i].idx = i;
	}

	/* Sort it by desc order of value and idx */
	qsort(vwi, MAP_N_RANGES, sizeof(ValueWithIndex), vwi_compare);

	cnt = 0;
	for (i = 0; i < N_CHOOSE; i++)
	{
		BlockNumber start, end;

		if (vwi[i].value <= 0)
			break;

		gmaprel_get_range(vwi[i].idx, &start, &end);
		start = Max(start, 0);
		end = Min(end, RelationGetNumberOfBlocks(onerel) - 1);

		result[cnt++] = start;
		result[cnt++] = end;
		elog(INFO, "map[%d] = %d, start %u, end %u", vwi[i].idx, vwi[i].value,
			 start, end);
	}
	elog(INFO, "-------------------------------------");

	result[cnt] = InvalidBlockNumber;

	return result;
}
