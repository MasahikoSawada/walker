# src/bin/walw/Makefile

MODULES = walw heatmap
PGFILEDESC = "walw"

OBJS = walw.o xlogreader.o heatmap.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/bin/walw
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
