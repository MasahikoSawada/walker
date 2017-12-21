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

struct WALkerCallbacks;

typedef void (*WALkerPluginInit) (struct WALkerCallbacks *cb);

/* Callback Functions */
typedef void (*WALkerCallbackStartup_cb) (void);
typedef void (*WALkerCallbackHeap_cb) (XLogReaderState *record);
typedef void (*WALkerCallbackHeap2_cb) (XLogReaderState *record);
typedef void (*WALkerCallbackXlog_cb) (XLogReaderState *record);
typedef void (*WALkerCallbackXact_cb) (XLogReaderState *record);

/* Struct containing callback functions */
typedef struct WALkerCallbacks
{
	WALkerCallbackStartup_cb	startup_cb;
	WALkerCallbackHeap_cb		heap_cb;
	WALkerCallbackHeap2_cb		heap2_cb;
	WALkerCallbackXlog_cb		xlog_cb;
	WALkerCallbackXact_cb		xact_cb;
} WALkerCallbacks;
