/*-------------------------------------------------------------------------
 *
 * heatmap.c - Heatmap generator for heap relations
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  walker/heatmap.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "walker.h"

/* These are always necessary for a bgworker */
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
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* Heatmap array size */
#define MAP_SIZE 1024

/* Each MAP_RANGE_SIZE blocks belong to same index */
#define MAP_RANGE_SIZE 32

#define TargetBlknoToIndex(blkno) (blkno / MAP_RANGE_SIZE)

typedef struct Heatmap
{
	RelFileNode	node;
	int			hmap[MAP_SIZE];
} Heatmap;

/* Plugin handler function */
extern void _PG_walker_plugin_init(WALkerCallbacks *cb);

/* Callback functions */
static void heatmap_startup(void);
static void heatmap_heap(XLogReaderState *record);
static void heatmap_heap2(XLogReaderState *record);
static void heatmap_xlog(XLogReaderState *record);
static void heatmap_xact(XLogReaderState *record);
static void heatmap_smgr(XLogReaderState *record);

static void HeatmapRecordChange(RelFileNode node, BlockNumber blkno,
								int temp);
static void HeatmapClearChange(RelFileNode node, BlockNumber blkno);
static void HeatmapClearAllChanges(RelFileNode node);
static void HeatmapLogSummary(void);
static char *HeatmapDump(int *heatmap);

static HTAB *HeatmapHash;

/* Handler function */
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
	info.entrysize = sizeof(Heatmap);

	HeatmapHash = hash_create("Heapmap hash",
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

	switch (info)
	{
		/* Make one page dirty */
		case XLOG_HEAP_INSERT:
		case XLOG_HEAP_DELETE:
		case XLOG_HEAP_HOT_UPDATE:
		case XLOG_HEAP_LOCK:
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			HeatmapRecordChange(node, blkno, 1);
			break;

		/* Make two pages dirty */
		case XLOG_HEAP_UPDATE:
			{
				BlockNumber newblkno, oldblkno;

				XLogRecGetBlockTag(record, 0, &node, NULL, &newblkno);
				XLogRecGetBlockTag(record, 1, NULL, NULL, &oldblkno);
				HeatmapRecordChange(node, newblkno, 1);
				HeatmapRecordChange(node, oldblkno, 1);
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
 * Process RM_HEAP_ID record.
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
			HeatmapRecordChange(node, blkno, -1);

		/* Make one page clear */
		case XLOG_HEAP2_VISIBLE:
			XLogRecGetBlockTag(record, 1, &node, NULL, &blkno);
			HeatmapClearChange(node, blkno);
			break;

		/* Make one page dirty */
		case XLOG_HEAP2_MULTI_INSERT:
		case XLOG_HEAP2_LOCK_UPDATED:
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			HeatmapRecordChange(node, blkno, 1);
			break;

		/* Ignore */
		case XLOG_HEAP2_NEW_CID:
			break;
		default:
			elog(ERROR, "unexpected RM_HEAP2_ID record type: %u", info);
			break;
	}
}

static void
heatmap_xlog(XLogReaderState *record)
{
	uint8	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch(info)
	{
		/* Report summary of heatmap */
		case XLOG_CHECKPOINT_ONLINE:
			HeatmapLogSummary();
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
				HeatmapClearAllChanges(xlrec->rnode);
				break;
			}
		default:
			elog(ERROR, "unexpected RM_SMGR_ID record type: %u", info);
			break;
	}
}

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
						HeatmapClearAllChanges(parsed.xnodes[i]);
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
 * Record heap block changes of each relations. Increment
 * the temprature of given relation per this function calls.
 */
static void
HeatmapRecordChange(RelFileNode node, BlockNumber blkno, int temp)
{
	int index;
	Heatmap *hm;
	bool	found;

	/* Skip if the given relation is biltins */
	if (node.relNode < FirstNormalObjectId)
		return;

	hm = (Heatmap *) hash_search(HeatmapHash,
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

	index = TargetBlknoToIndex(blkno);

	/* XXX : Make sure we don't overflow */
	Assert(index < MAP_SIZE);

	/* Record the temprature of this range */
	hm->hmap[index] += temp;
}

/*
 * Claer heap block changes of each relations. If we decete
 * the WALs that implies the given block doesn't has any garbage
 * we can clear its temprature.
 */
static void
HeatmapClearChange(RelFileNode node, BlockNumber blkno)
{
	int index;
	Heatmap *hm;
	bool	found;

	hm = (Heatmap *) hash_search(HeatmapHash,
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

	index = TargetBlknoToIndex(blkno);

	/* XXX : Make sure we don't overflow */
	Assert(index < MAP_SIZE);

	/* Clear temprature of this range */
	hm->hmap[index] = 0;
}

static void
HeatmapClearAllChanges(RelFileNode node)
{
	Heatmap *hm;
	bool	found;

	hm = (Heatmap *) hash_search(HeatmapHash,
								 (void *) &node,
								 HASH_ENTER, &found);

	/* Initialize */
	if (!found)
	{
		hm->node.spcNode = node.spcNode;
		hm->node.dbNode = node.dbNode;
		hm->node.relNode = node.relNode;
	}

	memset(hm->hmap, 0, MAP_SIZE);
}

/* Logging heatmap summary for debug purpose */
static void
HeatmapLogSummary(void)
{
	HASH_SEQ_STATUS status;
	Heatmap *entry;

	hash_seq_init(&status, HeatmapHash);

	while ((entry = (Heatmap *) hash_seq_search(&status)) != NULL)
	{
		char *dumped_map = HeatmapDump(entry->hmap);
		RelFileNode node = entry->node;

		ereport(LOG,
				(errmsg("(%d,%d,%d) %s",
						node.spcNode, node.dbNode,node.relNode,
						dumped_map)));
	}
}

static char *
HeatmapDump(int *heatmap)
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
