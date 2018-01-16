# WALker (WAL Walker)

A simple, pluggable background worker for PostgreSQL, walking over WAL.

WALker is a background worker for PostgreSQL. It keeps walking over generated WAL and read it. WALker itself dones't do any actions, it just identifies the kind of WALs and invokes a corresponding callback of plugins.

# Installation
Since WALker requires PostgreSQL source code for compilation, please download PostgreSQL source code from [here](https://www.postgresql.org/ftp/source/). The WALker support PostgreSQL 10.0 or higher.

1. Extract PostgreSQL source code and go to contrib/ directory
```bash
$ tar zxf postgresql-10.1.tar.bz2
$ cd postgresql-10.1/contrib
```
2. git clone WALker repositry and build
```bash
$ git clone git@github.com:MasahikoSawada/walker.git
$ cd walker
$ make
$ su
# make install
```

# Usage

## Setting
WALker provides only one GUC parameter `walker.plugins`. Setting comma-separated plugins list to `walker.plugins` to postgresql.conf

```bash
$ vim /path/to/postgresql.conf
shared_preload_libraries = 'walker'
walker.plugins = 'garbagemap'
```
# WALker Plugins
WALker is designed to be used together with multile exernal functionalities, WALker itself doesn't do any action. This repository has [garbagemap](https://github.com/MasahikoSawada/walker/tree/master/garbagemap) plugin. Please refer it as an example.

## Requirment
WALker plugin has to include `walker.h`.

## Initialize function
An WALker plugin is loaded by dynamically loading a shared library with the plugin's name as the library base name. The normal library search path is used to locate the library. To provide the required output plugin callbacks and to indicate that the library is actually an output plugin it needs to provide a function named _PG_walker_plugin_init. This function is passed a struct that needs to be filled with the callback function pointers for individual actions.

```c
typedef struct WalkerCallbacks
{
	WalkerCallbackStartup_cb	startup_cb;
	WalkerCallbackHeap_cb		heap_cb;
	WalkerCallbackHeap2_cb		heap2_cb;
	WalkerCallbackXlog_cb		xlog_cb;
	WalkerCallbackXact_cb		xact_cb;
} WalkerCallbacks;

typedef void (*WalkerPluginInit) (struct WalkerCallbacks *cb);
```

## WALker Plugin Callbacks
All callback funcitons are optional. If multiple plugins are specified, each callbacks is called in same order as setting.

### Callback for Startup
This callback is called just after dynamically loaded at initialization step.

```c
typedef void (*WalkerCallbackStartup_cb) (void);
```

### Callback for Resoource Manager
WALker identifies the WAL record and invoke corresponding callbacks.

```c
typedef void (*WalkerCallbackHeap_cb) (XLogReaderState *record);
```

### Callback for RM_HEAP2_ID

```c
typedef void (*WalkerCallbackHeap2_cb) (XLogReaderState *record);
```

### Callback for RM_XLOG_ID

```c
typedef void (*WalkerCallbackXlog_cb) (XLogReaderState *record);
```

### Callback for RM_XACT_ID

```c
typedef void (*WalkerCallbackXact_cb) (XLogReaderState *record);
```

### Callback for RM_SMGR_ID

```c
typedef void (*WalkerCallbackSmgr_cb) (XLogReaderState *record);
```

# FAQ
* Is the WALker same as logical decoding plugin?
  * No, at least now. The Logical decoding plugins cannot retrieve WALs of wihch correponding transaction is rollbacked or aborted. Also, logical decoding plugin's function are invoked at commit of the transaction. On the other hand, WALker reads through all WAL record including both aborted record and committed record. Also, WALker doesn't put a restriction regarding GUC parameters.
* What can we use WALker for?
  * I think WALker has unlimited possibilities. This repository has a sample plugin called `garbagemap`. This plugin collect garbage information of all heap and generate heat map which helps us to reclaim garbage more effeciency.
