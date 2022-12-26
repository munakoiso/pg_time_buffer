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
#include "storage/lwlock.h"

#include "pg_time_buffer_constants.h"

/*
 * We do not know size of external key, that's why we use FLEXIBLE_ARRAY_MEMBER here
 * Actual structure is: [<key_size bytes of external key>, bucket]
 * */
typedef struct pgtbKey {
    int  bucket;
    char key[FLEXIBLE_ARRAY_MEMBER];
} pgtbKey;

typedef struct pgtbMeta {
    int     count;
    slock_t mutex;
} pgtbMeta;

typedef struct pgtbGlobalInfo {
    int current_bucket;
    int bucket_fullness[actual_buckets_count]; /* count of unique external keys in every bucket */
    int bucket_duration; /* duration of one bucket in seconds */
    int items_count; /* external keys capacity of one bucket */

    bool    out_of_shared_memory;
    bool    bucket_is_full; /* is current bucket full */
    slock_t bucket_overflow_mutex; /* protects bucket_is_full, bucket_overflow_by */
    int     bucket_overflow_by;
    char    extension_name[max_extension_name_length];
    HTAB*   data_htab; /* link to hash table that maps pgtbKey into [pgtbKey, external_value, pgtbMeta] */

    TimestampTz init_timestamp;
    TimestampTz last_update_timestamp;

    slock_t get_stats_lock; /* protects bucket with index -1 from concurrent operations */
    LWLock  lock; /* protects global info */

    int key_size; /* external key size */
    int value_size; /* external value size */
    int htab_key_size; /* size of key in hash table (external key size + sizeof(pgtbKey)) */
    int data_htab_value_size; /* size of data in hash table (htab_key_size + external value size + sizeof(pgtbMeta)) */

    void (*add)(void* a, void* b); /* inplace adds b to a */
    void (*on_delete)(void* key, void* value); /* function that called on key deletion (when key is outdated) */

    /* Ring-buffer that contains external keys. Logically separated by 'buckets'.
     * Every bucket contains keys written in the corresponding time interval of length bucket_duration seconds */
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
