EXTENSION = pg_tap_counter
MODULE_big = pg_tap_counter
OBJS = pg_tap_counter.o pg_time_buffer.o
PG_CONFIG = pg_config
DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
