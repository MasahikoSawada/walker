/*-------------------------------------------------------------------------
 *
 * walker.c - WAL WALker
 *
 * Copyright (c) 2013-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  walker/walker.c
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
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

/* WALker state data */
typedef struct WALkerStateData
{
	List	*plugins;	 /* List of WALkerCallbacks */
} WALkerStateData;

void _PG_init(void);
void WALkerMain(Datum main_arg);

static void WALkerProcessRecord(XLogReaderState *record);
static struct WALkerStateData *WALkerState;

/* GUC parameters */
static char *walker_plugins;

/* flags set by signal handlers */
sig_atomic_t got_sighup = false;

/*
 * Signal handler for SIGHUP.
 */
static void
walker_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

/*
 * Initialize function.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("walker.plugins",
							   "comma-separated plugin names",
							   NULL,
							   &walker_plugins,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	strcpy(worker.bgw_library_name, "walker");
	strcpy(worker.bgw_function_name, "WALkerMain");
	worker.bgw_notify_pid = 0;

	snprintf(worker.bgw_name, BGW_MAXLEN, "walker");
	worker.bgw_main_arg = Int32GetDatum(1);
	RegisterBackgroundWorker(&worker);
}

/*
 * Initialize WALker's space. Also we load all given plugins here.
 */
static void
WALkerInit(void)
{
	MemoryContext ctx;
	List *plugin_list;
	ListCell *cell;

	if (!SplitIdentifierString(walker_plugins, ',', &plugin_list))
		elog(ERROR, "plugin syntax is invalid");

	ctx = MemoryContextSwitchTo(TopMemoryContext);

	/* Initialize global variables */
	WALkerState = (WALkerStateData *) palloc(sizeof(WALkerStateData));
	WALkerState->plugins = NIL;

	/* Iterate over all plugin names */
	foreach (cell, plugin_list)
	{
		char *plugin_name = (char *) lfirst(cell);
		WALkerPluginInit plugin_init;
		WALkerCallbacks *callbacks;

		callbacks = (WALkerCallbacks *) palloc(sizeof(WALkerCallbacks));
		plugin_init = (WALkerPluginInit)
			load_external_function(plugin_name, "_PG_walker_plugin_init", false, NULL);

		if (plugin_init == NULL)
			elog(ERROR, "output plugins have to declare the _PG_walker_plugin_init symbol");

		/* Call plugin's init function for walker */
		plugin_init(callbacks);

		/* startup_cb can be NULL while other callback must not be NULL */
		if (callbacks->heap_cb == NULL)
			elog(ERROR, "output plugins have to register a heap callback");
		if (callbacks->heap2_cb == NULL)
			elog(ERROR, "output plugins have to register a heap callback");

		/* Invoke startup callback if it's provided */
		if (callbacks->startup_cb)
			callbacks->startup_cb();

		/* Add to plugin list */
		WALkerState->plugins = lappend(WALkerState->plugins, callbacks);
	}

	MemoryContextSwitchTo(ctx);
}

/*
 * Entry point of walker background worker process.
 */
void
WALkerMain(Datum main_arg)
{
	XLogReaderState *xlogreader_state;
	XLogRecPtr	lsn = GetFlushRecPtr();
	XLogRecord *record;
	char *errmsg;

	pqsignal(SIGHUP, walker_sighup);
	pqsignal(SIGTERM, die);

	/* Initialize walker plugins */
	WALkerInit();

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

#if PG_VERSION_NUM >= 110000
	xlogreader_state = XLogReaderAllocate(wal_segment_size, &read_local_xlog_page,
										  NULL);
#else
	xlogreader_state = XLogReaderAllocate(&read_local_xlog_page, NULL);
#endif

	if (!xlogreader_state)
		elog(ERROR, "failed to allocate xlog reader");

	/* Loop until get SIGTERM */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/* Got SIGHUP, relad configuration file */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Read a record. Wait for new record if it is not generated yet */
		record = XLogReadRecord(xlogreader_state, lsn, &errmsg);
		lsn = InvalidXLogRecPtr;

		if (record == NULL)
			elog(ERROR, "could not read WAL at %X/%X",
				 (uint32) (lsn >> 32), (uint32) lsn);

		WALkerProcessRecord(xlogreader_state);
	}
}

static void
WALkerProcessRecord(XLogReaderState *record)
{
	ListCell *cell;

	foreach (cell, WALkerState->plugins)
	{
		WALkerCallbacks *cb = (WALkerCallbacks *) lfirst(cell);
		/* cast so we get a warning when new rmgrs are added */
		switch ((RmgrIds) XLogRecGetRmid(record))
		{
			/*
			 * Rmgrs we care about for logical decoding. Add new rmgrs in
			 * rmgrlist.h's order.
			 */
			case RM_HEAP2_ID:
			{
				if (cb->heap2_cb)
					cb->heap2_cb(record);
			}
			break;
			case RM_HEAP_ID:
			{
				if (cb->heap_cb)
					cb->heap_cb(record);
			}
			break;
			case RM_XLOG_ID:
			{
				if (cb->xlog_cb)
					cb->xlog_cb(record);
			}
			case RM_XACT_ID:
			{
				if (cb->xact_cb)
					cb->xact_cb(record);
			}
			case RM_STANDBY_ID:
			case RM_LOGICALMSG_ID:
			case RM_SMGR_ID:
			case RM_CLOG_ID:
			case RM_DBASE_ID:
			case RM_TBLSPC_ID:
			case RM_MULTIXACT_ID:
			case RM_RELMAP_ID:
			case RM_BTREE_ID:
			case RM_HASH_ID:
			case RM_GIN_ID:
			case RM_GIST_ID:
			case RM_SEQ_ID:
			case RM_SPGIST_ID:
			case RM_BRIN_ID:
			case RM_COMMIT_TS_ID:
			case RM_REPLORIGIN_ID:
			case RM_GENERIC_ID:
				break;
			case RM_NEXT_ID:
				elog(ERROR, "unexpected RM_NEXT_ID rmgr_id: %u", (RmgrIds) XLogRecGetRmid(record));
		}
	}
}
