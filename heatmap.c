/*-------------------------------------------------------------------------
 *
 * heatmap.c - Heatmap generator for HEAP
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  walw/heatmap.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "walw.h"

/* These are always necessary for a bgworker */
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/transam.h"
#include "access/heapam_xlog.h"
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

/* Plugin handler function */
extern void _PG_walw_plugin_init(WalwCallbacks *cb);

/* Callback functions */
static void heatmap_heap(XLogReaderState *record);
static void heatmap_heap2(XLogReaderState *record);

/* Handler function */
void
_PG_walw_plugin_init(WalwCallbacks *cb)
{
	cb->heap_cb = heatmap_heap;
	cb->heap2_cb = heatmap_heap2;
}

/*
 * Process RM_HEAP_ID record.
 */
static void
heatmap_heap(XLogReaderState *record)
{
	uint8			info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_HEAP_INSERT:
		case XLOG_HEAP_HOT_UPDATE:
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_DELETE:
		case XLOG_HEAP_INPLACE:
		case XLOG_HEAP_CONFIRM:
		case XLOG_HEAP_LOCK:
		{
			xl_heap_insert *xlrec;
			RelFileNode node;
			BlockNumber	blkno;

			xlrec = (xl_heap_insert *) XLogRecGetData(record);
			XLogRecGetBlockTag(record, 0, &node, NULL, &blkno);
			ereport(LOG, (errmsg("heap: (%d, %d, %d) %d",
								 node.spcNode, node.dbNode, node.relNode,
								 blkno)));
		}
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
