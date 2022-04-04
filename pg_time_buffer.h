#include "postgres.h"

#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/jsonb.h"
#include "executor/execdesc.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"

#include "pg_time_buffer_constants.h"

typedef struct pgtbKey {
    int bucket;
    char key[FLEXIBLE_ARRAY_MEMBER];
} pgtbKey;

typedef struct pgtbMeta {
    int count;
} pgtbMeta;

typedef struct pgtbGlobalInfo {
    int current_bucket;
    int bucket_fullness[actual_buckets_count];
    int bucket_duration;
    /* count of keys in one bucket */
    int items_count;

    bool out_of_shared_memory;
    bool bucket_is_full;
    int bucket_overflow_by;
    char extension_name[max_extension_name_length];
    HTAB* data_htab;

    TimestampTz init_timestamp;
    TimestampTz last_update_timestamp;

    LWLock value_htab_lock;
    LWLock buffer_lock;

    int buffer_size;
    int stats_size;
    int key_size;
    int htab_key_size;
    int data_htab_value_size;
    int value_size;

// inplace add b to a
    void (*add)(void* a, void* b);
    void (*on_delete)(void* key, void* value);

    char buckets;
} pgtbGlobalInfo;

void pgtb_init(const char*,
               void (*add)(void*, void*),
               void (*on_delete)(void*, void*),
               int,
               uint64_t,
               uint64_t,
               uint64_t
               );
bool pgtb_put(const char*, void*, void*);
void pgtb_tick(const char*);
void pgtb_get_stats(const char*, void*, int*, TimestampTz*, TimestampTz*);
void pgtb_get_stats_time_interval(const char*, TimestampTz*, TimestampTz*, void*, int*);
int pgtb_get_items_count(uint64_t, uint64_t, uint64_t);
