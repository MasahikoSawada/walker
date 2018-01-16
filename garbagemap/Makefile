# src/bin/garbagemap/Makefile

MODULES = garbagemap
PGFILEDESC = "garbagemap"
walkerdir = ../

EXTENSION = garbagemap
DATA = garbagemap--1.0.sql

override CPPFLAGS := -I$(walkerdir) $(CPPFLAGS)
OBJS = garbagemap.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/walker/garbagemap
top_builddir = ../../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
