# Walw

A pluggable background worker walking through WAL records.

# Usage

## Installation
Since Walw requires PostgreSQL source codes to build please download PostgreSQL source code from [here](https://www.postgresql.org/ftp/source/). The walw support PostgreSQL 10.0 or highe

1. Extract PostgreSQL source code and go to contrib/ directory

```bash
$ tar zxf postgresql-10.1.tar.bz2
$ cd postgresql-10.1/contrib
```

1. git clone walw repositry and build

```bash
$ git clone
$ make
$ su
# make install
```

## Setting
Walw provide only one GUC parameter `walw.plugins`. Setting comma-separated plugins list to `walw.plugins` to postgresql.conf

```bash
$ vim /path/to/postgresql.conf
shared_preload_libraries = 'walw'
walw.plugins = 'heatmap'
```
# Walw Plugins

## Initialize function
An Walw plugin is loaded by dinamically loading a shared library with the plugin's name as the library base name.  The normal library search path is used to locate the library. To provide the required output plugin callbacks and to indicate that the library is actually an output plugin it needs to provide a function named `_PG_walw_plugin_init`. This function is passed a struct that needs to be filled with the callback function pointers for individual actions.

```c
typedef struct WalwCallbacks
{
	WalwCallbackHeap_cb	heap_cb;
	WalwCallbackHeap2_cb	heap2_cb;
} WalwCallbacks;

typedef void (*WalwPluginInit) (struct WalwCallbacks *cb);
```

All callbacked are required.

## Walw Plugin Callbacks
The all callback functions are called whenever walw background worker read WAL record. Walw identifies the WAL record and dispatches it to appropriate callbacks. If multiple plugins are specified, each callbacks is called in same order as setting.

### Callback for RM_HEAP_ID

```c
typedef void (*WalwCallbackHeap_cb) (XLogReaderState *record);
```

### Callback for RM_HEAP2_ID

```c
typedef void (*WalwCallbackHeap2_cb) (XLogReaderState *record);
```
