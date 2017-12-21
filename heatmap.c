/*-------------------------------------------------------------------------
 *
 * heatmap.c - Heatmap generator for HEAP
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
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/transam.h"
#include "access/heapam_xlog.h"
#include "lib/dshash.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
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
	char		hmap[MAP_SIZE];
} Heatmap;

/* Plugin handler function */
extern void _PG_walker_plugin_init(WalkerCallbacks *cb);

/* Callback functions */
static void heatmap_startup(void);
static void heatmap_heap(XLogReaderState *record);
static void heatmap_heap2(XLogReaderState *record);

static void HeatmapRecordChange(RelFileNode node, BlockNumber blkno);

static HTAB *HeatmapHash;

/* Handler function */
void
_PG_walker_plugin_init(WalkerCallbacks *cb)
{
	cb->startup_cb = heatmap_startup;
	cb->heap_cb = heatmap_heap;
	cb->heap2_cb = heatmap_heap2;
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
		case XLOG_HEAP_INSERT:
		{
			xl_heap_insert *xlrec;

			xlrec = (xl_heap_insert *) XLogRecGetData(record);
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			HeatmapRecordChange(node, blkno);
		}
		break;
		case XLOG_HEAP_HOT_UPDATE:
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_DELETE:
		case XLOG_HEAP_INPLACE:
		case XLOG_HEAP_CONFIRM:
		case XLOG_HEAP_LOCK:
			break;
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
}

static void
HeatmapRecordChange(RelFileNode node, BlockNumber blkno)
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

	/* Increment temprature of the range */
	hm->hmap[index]++;

	{
		int i;
		for (i = 0; i < MAP_SIZE; i++)
		{
			if (hm->hmap[i] > 0)
				ereport(LOG, (errmsg("node = %d, [%d] = %d",
									 node.relNode, i, hm->hmap[i])));
		}
	}
}
