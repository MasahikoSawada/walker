# src/bin/walw/Makefile

MODULES = walw heatmap
PGFILEDESC = "walw"

OBJS = walw.o xlogreader.o heatmap.o

subdir = src/bin/walw
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
