#include "postgres.h"

#include <math.h>
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/datetime.h"
#include "utils/dynahash.h"
#include "utils/timestamp.h"
#include "pg_time_buffer.h"

static void pgtb_get_global_info(const char*, int, pgtbGlobalInfo**, bool*);
static void pgtb_pop_key(pgtbGlobalInfo*, int, int);
static TimestampTz pgtb_normalize_ts(pgtbGlobalInfo*, TimestampTz);
static void pgtb_internal_get_stats_time_interval(pgtbGlobalInfo*, TimestampTz*, TimestampTz*, void*, int*);
static void pgtb_reset(pgtbGlobalInfo*);

static uint64_t
pgtb_find_optimal_items_count(uint64_t left_bound,
                              uint64_t right_bound,
                              uint64_t* buffer_item,
                              uint64_t* data_htab_item,
                              uint64_t* total_size) {
    uint64_t buffer_size;
    uint64_t data_htab_size;
    uint64_t middle;

    middle = (right_bound + left_bound) / 2;
    buffer_size = *buffer_item * middle;
    data_htab_size = hash_estimate_size(middle, *data_htab_item);

    if (left_bound + 1 == right_bound) {
        return left_bound;
    }

    if (buffer_size + data_htab_size > *total_size) {
        return pgtb_find_optimal_items_count(left_bound,
                                             middle,
                                             buffer_item,
                                             data_htab_item,
                                             total_size);
    } else {
        return pgtb_find_optimal_items_count(middle,
                                             right_bound,
                                             buffer_item,
                                             data_htab_item,
                                             total_size);
    }
}

static void
pgtb_get_items_count_and_sizes(uint64_t total_size,
                                    uint64_t key_size,
                                    uint64_t value_size,
                                    uint64_t* htab_key_size,
                                    int* global_vars_size,
                                    uint64_t* data_htab_value_size,
                                    uint64_t* buffer_item_size,
                                    int* items_count) {
    uint64_t actual_size;
    uint64_t total_item_size;
    u_int64_t max_items_count;
    u_int64_t data_htab_item;

    *global_vars_size = sizeof(pgtbGlobalInfo);
    *htab_key_size = key_size + sizeof(pgtbKey);
    *data_htab_value_size = *htab_key_size + value_size + sizeof(pgtbMeta);
    *buffer_item_size = key_size;
    actual_size = total_size - *global_vars_size;
    /* Item size here - size of one key-value pair in all structures */
    total_item_size = (*data_htab_value_size + *htab_key_size) + *buffer_item_size;
    max_items_count = actual_size / total_item_size;
    data_htab_item = *htab_key_size + *data_htab_value_size;
    *items_count = (int)pgtb_find_optimal_items_count(0,
                                                     max_items_count,
                                                     buffer_item_size,
                                                     &data_htab_item,
                                                     &actual_size);
    *items_count = (int)(*items_count / (actual_buckets_count + 1));
}

int pgtb_get_items_count(uint64_t total_size,
                         uint64_t key_size,
                         uint64_t value_size) {
    int global_vars_size;
    u_int64_t htab_key_size;
    u_int64_t data_htab_value_size;
    u_int64_t buffer_item_size;
    int items_count;

    pgtb_get_items_count_and_sizes(total_size,
                                   key_size,
                                   value_size,
                                   &htab_key_size,
                                   &global_vars_size,
                                   &data_htab_value_size,
                                   &buffer_item_size,
                                   &items_count);
    return items_count;
}

static void
pgtb_get_global_info(const char* extension_name, int size, pgtbGlobalInfo** global_info, bool* found) {
    char global_vars_name[max_extension_name_length + max_additional_info_length];
    char size_name[max_extension_name_length + max_additional_info_length];
    int* size_ptr;
    bool size_found;

    memset(&global_vars_name, '\0', max_extension_name_length + max_additional_info_length);
    strlcpy(global_vars_name, extension_name, max_extension_name_length);
    strcat(global_vars_name, " pgtb global_vars");

    memset(&size_name, '\0', max_extension_name_length + max_additional_info_length);
    strlcpy(size_name, extension_name, max_extension_name_length);
    strcat(size_name, " pgtb size");

    /* We need to store size of global_info in shmem, but not in global info,
     * because we know size of it only while initialization */
    size_ptr = ShmemInitStruct(size_name, sizeof(int), &size_found);
    if (!size_found) {
        *size_ptr = size;
    }

    *global_info = ShmemInitStruct(global_vars_name, *size_ptr, found);
}

bool
pgtb_put(const char* extension_name, void* key_ptr, void* value_ptr) {
    pgtbGlobalInfo* global_info_ptr;
    pgtbKey* pgtb_key_ptr;
    char* data_ptr;
    bool found;
    int index;
    pgtb_get_global_info(extension_name, 0, &global_info_ptr, &found);
    if (!found) {
        return false;
    }
    LWLockAcquire(&global_info_ptr->buffer_lock, LW_EXCLUSIVE);
    LWLockAcquire(&global_info_ptr->value_htab_lock, LW_EXCLUSIVE);
    pgtb_key_ptr = (pgtbKey*) palloc(global_info_ptr->htab_key_size);
    memcpy(pgtb_key_ptr, key_ptr, global_info_ptr->key_size);
    *((int*)((char*)pgtb_key_ptr + global_info_ptr->key_size)) = global_info_ptr->current_bucket;
    data_ptr = hash_search(global_info_ptr->data_htab, (void *) pgtb_key_ptr, HASH_FIND, &found);
    if (!found) {
        if (global_info_ptr->bucket_fullness[global_info_ptr->current_bucket] == global_info_ptr->items_count - 1 ||
                global_info_ptr->out_of_shared_memory) {
            if (global_info_ptr->bucket_fullness[global_info_ptr->current_bucket] == global_info_ptr->items_count - 1) {
                global_info_ptr->bucket_is_full = true;
                global_info_ptr->bucket_overflow_by += 1;
            }
            pfree(pgtb_key_ptr);
            LWLockRelease(&global_info_ptr->value_htab_lock);
            LWLockRelease(&global_info_ptr->buffer_lock);
            return false;
        }
        data_ptr = hash_search(global_info_ptr->data_htab, (void *) pgtb_key_ptr, HASH_ENTER_NULL, &found);
        if (data_ptr == NULL) {
            global_info_ptr->out_of_shared_memory = true;
            pfree(pgtb_key_ptr);
            LWLockRelease(&global_info_ptr->value_htab_lock);
            LWLockRelease(&global_info_ptr->buffer_lock);
            return false;
        }
        memcpy(data_ptr, pgtb_key_ptr, global_info_ptr->htab_key_size);
        memcpy((char*)data_ptr + global_info_ptr->htab_key_size, value_ptr, global_info_ptr->value_size);

        index = global_info_ptr->current_bucket * global_info_ptr->items_count
                + global_info_ptr->bucket_fullness[global_info_ptr->current_bucket];
        memcpy((char*)(&global_info_ptr->buckets) + index * global_info_ptr->key_size,
               pgtb_key_ptr, global_info_ptr->key_size);

        global_info_ptr->bucket_fullness[global_info_ptr->current_bucket] += 1;
    } else {
        global_info_ptr->add((char*)data_ptr + global_info_ptr->htab_key_size, value_ptr);
    }
    *((int*)((char*)data_ptr + global_info_ptr->htab_key_size + global_info_ptr->value_size)) += 1;

    pfree(pgtb_key_ptr);
    LWLockRelease(&global_info_ptr->value_htab_lock);
    LWLockRelease(&global_info_ptr->buffer_lock);
    return true;
}

void
pgtb_init(
        const char* extension_name,
        void (*add)(void*, void*),
        void (*on_delete)(void*, void*),
        int tick_interval,
        u_int64_t total_size,
        u_int64_t key_size,
        u_int64_t value_size
        ) {
    bool found;
    HASHCTL info;
    int buffer_size;

    int global_vars_size;
    u_int64_t htab_key_size;
    u_int64_t data_htab_value_size;
    u_int64_t buffer_item_size;
    int items_count;

    int htab_entries_count;

    pgtbGlobalInfo *global_info = NULL;

    char data_htab_name[max_extension_name_length + max_additional_info_length];

    pgtb_get_items_count_and_sizes(total_size,
                                   key_size,
                                   value_size,
                                   &htab_key_size,
                                   &global_vars_size,
                                   &data_htab_value_size,
                                   &buffer_item_size,
                                   &items_count);

    elog(LOG, "pgtb: %s. Max count of items: %d", extension_name, items_count);

    buffer_size = buffer_item_size * items_count * (actual_buckets_count + 1);

    pgtb_get_global_info(extension_name, buffer_size + global_vars_size, &global_info, &found);

    global_info->data_htab = NULL;
    global_info->bucket_overflow_by = 0;
    global_info->bucket_duration = tick_interval;
    global_info->buffer_size = buffer_size;
    global_info->items_count = items_count;
    global_info->key_size = key_size;
    global_info->htab_key_size = htab_key_size;
    global_info->data_htab_value_size = data_htab_value_size;
    global_info->value_size = value_size;
    memset(&global_info->extension_name, '\0', max_extension_name_length);
    memcpy(&global_info->extension_name, extension_name, max_extension_name_length);
    global_info->add = add;
    global_info->on_delete = on_delete;
    global_info->out_of_shared_memory = false;

    htab_entries_count = items_count * (actual_buckets_count + 1);
    memset(&info, 0, sizeof(info));
    info.keysize = global_info->htab_key_size;
    info.entrysize = global_info->data_htab_value_size;

    memset(&data_htab_name, '\0', max_extension_name_length + max_additional_info_length);
    strlcpy(data_htab_name, extension_name, max_extension_name_length);
    strcat(data_htab_name, " pgtb data_htab");
    global_info->data_htab = ShmemInitHash(data_htab_name,
                                           htab_entries_count, htab_entries_count,
                                           &info,
                                           HASH_ELEM | HASH_BLOBS);
    pgtb_reset(global_info);
}

static void
pgtb_pop_key(pgtbGlobalInfo* global_info, int bucket, int key_in_bucket) {
    bool found;
    char *data;
    pgtbKey* key;
    int index;
    int htab_key_size;
    void (*on_delete)(void* key, void* value);
    char* data_copy;

    if (global_info == NULL)
        return;

    if (key_in_bucket >= global_info->bucket_fullness[bucket]) {
        return;
    }
    data_copy = palloc(global_info->htab_key_size + global_info->value_size);
    on_delete = global_info->on_delete;
    htab_key_size = global_info->htab_key_size;
    index = bucket * global_info->items_count + key_in_bucket;
    key = (pgtbKey*) palloc(global_info->htab_key_size);
    memcpy(key, (char*)&global_info->buckets + index * global_info->key_size, global_info->key_size);
    *((int*)((char*)key + global_info->key_size)) = bucket;
    data = hash_search(global_info->data_htab, (void *) key, HASH_FIND, &found);
    if (found) {
        *((int*)((char*)data + global_info->htab_key_size + global_info->value_size)) -= 1;
        memcpy(data_copy, data, global_info->htab_key_size + global_info->value_size);
        (*on_delete)(data_copy, (char*)data_copy + htab_key_size);
        data = hash_search(global_info->data_htab, (void *) key, HASH_REMOVE, &found);
    }
    pfree(key);
    pfree(data_copy);
}

static void
pgtb_reset(pgtbGlobalInfo* global_info) {
    LWLockInitialize(&global_info->value_htab_lock, LWLockNewTrancheId());
    LWLockInitialize(&global_info->buffer_lock, LWLockNewTrancheId());

    memset(&global_info->buckets,
           0,
           global_info->buffer_size);
    memset(&global_info->bucket_fullness, 0, sizeof(global_info->bucket_fullness));
    LWLockAcquire(&global_info->buffer_lock, LW_EXCLUSIVE);
    global_info->current_bucket = 0;
    LWLockRelease(&global_info->buffer_lock);
    global_info->init_timestamp = GetCurrentTimestamp();
}

void
pgtb_tick(const char* extension_name) {
    int next_bucket;
    int key_in_bucket;
    int64 stat_interval_microsec;
    bool found;
    pgtbGlobalInfo* global_info;

    pgtb_get_global_info(extension_name, 0, &global_info, &found);
    if (!found)
        return;
    LWLockAcquire(&global_info->buffer_lock, LW_EXCLUSIVE);
    next_bucket = (global_info->current_bucket + 1) % actual_buckets_count;
    LWLockRelease(&global_info->buffer_lock);
    for (key_in_bucket = 0; key_in_bucket < global_info->items_count; ++key_in_bucket) {
        pgtb_pop_key(global_info, next_bucket, key_in_bucket);
    }
    stat_interval_microsec = ((int64)global_info->bucket_duration) * actual_buckets_count * 1e6;
    LWLockAcquire(&global_info->buffer_lock, LW_EXCLUSIVE);
    global_info->current_bucket = next_bucket;
    global_info->bucket_fullness[next_bucket] = 0;
    global_info->last_update_timestamp = GetCurrentTimestamp();
    if (next_bucket == 0)
        global_info->init_timestamp = global_info->last_update_timestamp - stat_interval_microsec;
    if (global_info->bucket_is_full) {
        elog(WARNING, "pgtb (%s). Bucket overflow by %d (%f%%)",
             global_info->extension_name,
             global_info->bucket_overflow_by,
             (float)global_info->bucket_overflow_by / (global_info->items_count + 1) * 100);
    }
    if (global_info->out_of_shared_memory) {
        elog(WARNING, "pgtb: Out of shared memory");
        global_info->out_of_shared_memory = false;
    }
    global_info->bucket_overflow_by = 0;
    global_info->bucket_is_full = false;
    LWLockRelease(&global_info->buffer_lock);
}

void
pgtb_get_stats(const char* extension_name, void* result, int* length, TimestampTz* timestamp_left, TimestampTz* timestamp_right) {
    bool found;
    pgtbGlobalInfo* global_info;
    pgtb_get_global_info(extension_name, 0, &global_info, &found);
    /* Shmem structs not ready yet */
    if (!found) {
        elog(WARNING, "pgtb not inited yet");
        return;
    }

    *timestamp_right = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), global_info->bucket_duration * 1000);
    *timestamp_left = global_info->init_timestamp;

    pgtb_internal_get_stats_time_interval(global_info, timestamp_left, timestamp_right, result, length);
}

void
pgtb_get_stats_time_interval(const char* extension_name, TimestampTz* timestamp_left, TimestampTz* timestamp_right, void* result, int* length) {
    bool found;
    pgtbGlobalInfo* global_info;
    pgtb_get_global_info(extension_name, 0, &global_info, &found);
    /* Shmem structs not ready yet */
    if (!found) {
        elog(LOG, "pgtb not inited yet");
        return;
    }
    pgtb_internal_get_stats_time_interval(global_info, timestamp_left, timestamp_right, result, length);
}

static TimestampTz
pgtb_normalize_ts(pgtbGlobalInfo* global_info, TimestampTz ts) {
    long sec_diff;
    int msec_diff;
    TimestampTz now;
    now = GetCurrentTimestamp();
    if (global_info->init_timestamp > ts)
        ts = global_info->init_timestamp;
    TimestampDifference(ts, global_info->last_update_timestamp, &sec_diff, &msec_diff);
    /* if ts < oldest stat we have */
    if (sec_diff > buckets_count * global_info->bucket_duration) {
        ts = TimestampTzPlusMilliseconds(global_info->last_update_timestamp, - (int64) buckets_count * global_info->bucket_duration * 1e3);
    }
    if (ts > now) {
        ts = now;
    }

    return ts;
}

static void
pgtb_internal_get_stats_time_interval(pgtbGlobalInfo* global_info,
                                      TimestampTz* timestamp_left,
                                      TimestampTz* timestamp_right,
                                      void* result,
                                      int* count) {
    int bucket_index;
    int bucket_index_0;
    int delta_bucket;
    int key_index;
    int bucket_fullness;
    pgtbKey* key_ptr;
    char* data;
    char* data_tmp;
    bool found;
    int index;
    TimestampTz norm_left_ts;
    TimestampTz norm_right_ts;
    int bucket_left;
    int bucket_right;
    int bucket_interval;
    int64 bucket_time_since_init;
    int64 sec_diff;
    int msec_diff;
    int current_bucket;
    bool max_output;
    LWLockAcquire(&global_info->buffer_lock, LW_EXCLUSIVE);
    key_ptr = (pgtbKey*) palloc(global_info->htab_key_size);
    memset(key_ptr, 0, global_info->htab_key_size);
    norm_left_ts = pgtb_normalize_ts(global_info, *timestamp_left);
    TimestampDifference(global_info->init_timestamp, norm_left_ts, &sec_diff, &msec_diff);
    bucket_time_since_init = sec_diff / global_info->bucket_duration;
    norm_left_ts = TimestampTzPlusMilliseconds(global_info->init_timestamp, bucket_time_since_init * global_info->bucket_duration * 1e3);
    bucket_left = bucket_time_since_init % actual_buckets_count;

    norm_right_ts = pgtb_normalize_ts(global_info, *timestamp_right);
    TimestampDifference(global_info->init_timestamp, norm_right_ts, &sec_diff, &msec_diff);
    bucket_time_since_init = sec_diff / global_info->bucket_duration + 1;
    norm_right_ts = TimestampTzPlusMilliseconds(global_info->init_timestamp, bucket_time_since_init * global_info->bucket_duration * 1e3);

    if (norm_right_ts > GetCurrentTimestamp())
        norm_right_ts = GetCurrentTimestamp();

    bucket_right = bucket_time_since_init % actual_buckets_count;
    if (bucket_right > global_info->current_bucket ||
       (global_info->current_bucket == actual_buckets_count - 1 && bucket_right == 0))
        bucket_right = global_info->current_bucket;

    *timestamp_left = norm_left_ts;
    *timestamp_right = norm_right_ts;

    bucket_interval = (bucket_right - bucket_left + 1 + actual_buckets_count) % actual_buckets_count;
    bucket_index_0 = bucket_left;
    bucket_fullness = global_info->bucket_fullness[global_info->current_bucket];
    current_bucket = global_info->current_bucket;
    LWLockRelease(&global_info->buffer_lock);
    *count = 0;
    max_output = false;
    for (delta_bucket = 0; delta_bucket < bucket_interval; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < global_info->items_count; ++key_index) {
            if ((bucket_index == current_bucket && key_index == bucket_fullness) ||
                (bucket_index != current_bucket && key_index == global_info->bucket_fullness[bucket_index])) {
                break;
            }

            index = bucket_index * global_info->items_count + key_index;
            memcpy(key_ptr, (char*)&global_info->buckets + index * global_info->key_size, global_info->key_size);
            *((int*)((char*)key_ptr + global_info->key_size)) = bucket_index;

            data = hash_search(global_info->data_htab, (void *) key_ptr, HASH_FIND, &found);
            if (!found) {
                /* Should never reach that place */
                continue;
            }
            *((int*)((char*)key_ptr + global_info->key_size)) = -1;
            data_tmp = hash_search(global_info->data_htab, (void *) key_ptr, HASH_FIND, &found);
            if (!found) {
                *count += 1;
                if (*count >= global_info->items_count) {
                    if (!max_output) {
                        ereport(WARNING,
                                (errcode(ERRCODE_WARNING),
                                        errmsg("pgtb: can not show more than %d results.", global_info->items_count),
                                        errhint("pgtb: You can increase buffer size of %s", global_info->extension_name)));
                        max_output = true;
                    }
                    break;
                }
                data_tmp = hash_search(global_info->data_htab, (void *) key_ptr, HASH_ENTER_NULL, &found);
                if (data_tmp == NULL) {
                    global_info->out_of_shared_memory = true;
                    elog(ERROR, "pgtb: Out of shared memory");
                    break;
                }
                memcpy(data_tmp, data, global_info->data_htab_value_size);

                *((int*)((char*)data_tmp + global_info->key_size)) = -1;
            } else {
                global_info->add((char*)data_tmp + global_info->htab_key_size, (char*)data + global_info->htab_key_size);
            }
        }
    }

    *count = 0;
    /* put to tuplestore and clear bucket index -1 */
    for (delta_bucket = 0; delta_bucket < bucket_interval; ++delta_bucket) {
        bucket_index = (bucket_index_0 + delta_bucket) % actual_buckets_count;
        for (key_index = 0; key_index < global_info->items_count; ++key_index) {
            if ((bucket_index == current_bucket && key_index == bucket_fullness) ||
               (bucket_index != current_bucket && key_index == global_info->bucket_fullness[bucket_index])) {
                break;
            }
            index = bucket_index * global_info->items_count + key_index;
            memcpy(key_ptr, (char*)&global_info->buckets + index * global_info->key_size, global_info->key_size);
            *((int*)((char*)key_ptr + global_info->key_size)) = -1;

            data = hash_search(global_info->data_htab, (void *) key_ptr, HASH_FIND, &found);
            if (found) {
                memcpy((char*)result + *count * (global_info->key_size + global_info->value_size),
                       key_ptr,
                       global_info->key_size);
                memcpy((char*)result + *count * (global_info->key_size + global_info->value_size) + global_info->key_size,
                       (char*)data + global_info->htab_key_size,
                       global_info->value_size);
                *count += 1;
                data = hash_search(global_info->data_htab, (void *) key_ptr, HASH_REMOVE, &found);
            }
        }
    }

    pfree(key_ptr);
    return;
}
