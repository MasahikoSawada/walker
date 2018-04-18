# src/bin/walker/Makefile

MODULES = walker
PGFILEDESC = "walker"

subdir = contrib/walker
OBJS = walker.o xlogreader.o

SUBDIRS = garbagemap

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(recurse)
$(recurse_always)
