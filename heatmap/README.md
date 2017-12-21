# HeatMap

A plugin module of WALker. Heatmap generator for heap relations.


# Installation
HeatMap is a plugin module of WALker. HeatMap requires WALKer's codes to build.

Set `heatmap` to `walker.plugins` GUC parameter.

```bash
$ vim /path/to/postgresql.conf
shared_preload_libraries = 'walker'
walker.plugins = 'heatmap'
```

# Usage

## Generate heatmap for each heap relations
Using HeatMap, you can generate heatmap of each heap relations, which has a information of pages having dead tuples.

## Reporting heatmap summary
HeatMap logs the summary of all heatmaps whenever CHECKPOINT is executed to PostgreSQL sever log (LOG level). Example is,

```
LOG:  (1663,13194,16417) heatmap (0:13, 1248, 7232, 1520, 0:1008)
LOG:  (1663,13194,16407) heatmap (17, 16, 17, 19, 14, 21, 12, 20, 15, 17, 18, 15, 18, 112, 0:1010)
```
