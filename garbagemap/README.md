# GarbageMap

A plugin module of WALker. Garbage generator for heap relations.


# Installation
GarbageMap is a plugin module of WALker. GarbageMap requires WALKer's codes to build.

Set `garbagemap` to `walker.plugins` GUC parameter.

```bash
$ vim /path/to/postgresql.conf
shared_preload_libraries = 'walker, garbagemap'
walker.plugins = 'garbagemap'
```

# Usage
## Generate heatmap and histgram
```
$ psql
=# CREATE EXTENSION garbagemap;
$ sh gen_map.sh <db name> <table name>
```

Generate two graphes.

![images/gmap.png]

![images/ghist.png]

## WALker background worker 
### Generate garbagemap for each heap relations
Using GarbageMap, you can generate garbagemap of each heap relations, which has a information of pages having dead tuples.

### Reporting garbagemap summary
GarbageMap logs the summary of all garbagemaps whenever CHECKPOINT is executed to PostgreSQL sever log (LOG level). Example is,

```
LOG:  (1663,13194,16417) garbagemap (0:13, 1248, 7232, 1520, 0:1008)
LOG:  (1663,13194,16407) garbagemap (17, 16, 17, 19, 14, 21, 12, 20, 15, 17, 18, 15, 18, 112, 0:1010)
```
