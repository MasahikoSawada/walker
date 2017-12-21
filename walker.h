/* -------------------------------------------------------------------------
 *
 * walker.h
 *
 * -------------------------------------------------------------------------
 */

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "access/transam.h"
#include "access/heapam_xlog.h"

struct WalkerCallbacks;

typedef void (*WalkerPluginInit) (struct WalkerCallbacks *cb);

/* Callback Functions */
typedef void (*WalkerCallbackStartup_cb) (void);
typedef void (*WalkerCallbackHeap_cb) (XLogReaderState *record);
typedef void (*WalkerCallbackHeap2_cb) (XLogReaderState *record);
typedef void (*WalkerCallbackXlog_cb) (XLogReaderState *record);
typedef void (*WalkerCallbackXact_cb) (XLogReaderState *record);

/* Struct containing callback functions */
typedef struct WalkerCallbacks
{
	WalkerCallbackStartup_cb	startup_cb;
	WalkerCallbackHeap_cb		heap_cb;
	WalkerCallbackHeap2_cb		heap2_cb;
	WalkerCallbackXlog_cb		xlog_cb;
	WalkerCallbackXact_cb		xact_cb;
} WalkerCallbacks;
