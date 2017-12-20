/* -------------------------------------------------------------------------
 *
 * walw.h
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

struct WalwCallbacks;

typedef void (*WalwPluginInit) (struct WalwCallbacks *cb);

/* Callback Functions */
typedef void (*WalwCallbackStartup_cb) (void);
typedef void (*WalwCallbackHeap_cb) (XLogReaderState *record);
typedef void (*WalwCallbackHeap2_cb) (XLogReaderState *record);

typedef struct WalwCallbacks
{
	WalwCallbackStartup_cb	startup_cb;
	WalwCallbackHeap_cb		heap_cb;
	WalwCallbackHeap2_cb	heap2_cb;
} WalwCallbacks;
