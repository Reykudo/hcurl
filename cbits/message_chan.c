#include "message_chan.h"

#include "curl_uv.h"
#include "stream.h"

#include <limits.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

#define INITIAL_BUCKET_COUNT ((size_t)64)
#define SENDER_CLOSED_BIT ((size_t)1 << (sizeof(size_t) * CHAR_BIT - 1))
#define SENDER_PRODUCER_MASK (SENDER_CLOSED_BIT - 1)

struct message_sender_s {
    _Atomic size_t references;
    _Atomic size_t state;
    uv_mutex_t close_mutex;
    uv_cond_t producers_done;
    uv_async_t *async_handle;
    mpsc_t *queue;
};

static message_sender_t *message_sender_create(mpsc_t *queue) {
    message_sender_t *sender = calloc(1, sizeof(*sender));
    if (!sender) {
        return NULL;
    }
    atomic_init(&sender->references, 1);
    atomic_init(&sender->state, 0);
    sender->queue = queue;
    if (uv_mutex_init(&sender->close_mutex) != 0) {
        free(sender);
        return NULL;
    }
    if (uv_cond_init(&sender->producers_done) != 0) {
        uv_mutex_destroy(&sender->close_mutex);
        free(sender);
        return NULL;
    }
    return sender;
}

bool message_sender_retain(message_sender_t *sender) {
    if (!sender) {
        return false;
    }
    size_t references = atomic_load_explicit(&sender->references, memory_order_relaxed);
    do {
        if (references == 0 || references == SIZE_MAX) {
            return false;
        }
    } while (!atomic_compare_exchange_weak_explicit(
        &sender->references, &references, references + 1,
        memory_order_relaxed, memory_order_relaxed));
    return true;
}

void message_sender_release(message_sender_t *sender) {
    if (!sender) {
        return;
    }
    if (atomic_fetch_sub_explicit(&sender->references, 1, memory_order_acq_rel) == 1) {
        uv_cond_destroy(&sender->producers_done);
        uv_mutex_destroy(&sender->close_mutex);
        free(sender);
    }
}

message_sender_t *message_sender_acquire(uv_async_t *async_handle) {
    async_messages_context_t *context = async_handle ? async_handle->data : NULL;
    if (!context || !message_sender_retain(context->sender)) {
        return NULL;
    }
    return context->sender;
}

static bool message_sender_enter(message_sender_t *sender) {
    size_t state = atomic_load_explicit(&sender->state, memory_order_acquire);
    for (;;) {
        if ((state & SENDER_CLOSED_BIT) != 0
            || (state & SENDER_PRODUCER_MASK) == SENDER_PRODUCER_MASK) {
            return false;
        }
        if (atomic_compare_exchange_weak_explicit(
                &sender->state, &state, state + 1,
                memory_order_acquire, memory_order_relaxed)) {
            return true;
        }
    }
}

static void message_sender_leave(message_sender_t *sender) {
    size_t previous = atomic_fetch_sub_explicit(&sender->state, 1, memory_order_release);
    if ((previous & SENDER_CLOSED_BIT) != 0
        && (previous & SENDER_PRODUCER_MASK) == 1) {
        uv_mutex_lock(&sender->close_mutex);
        uv_cond_signal(&sender->producers_done);
        uv_mutex_unlock(&sender->close_mutex);
    }
}

static void message_sender_close(message_sender_t *sender) {
    if (!sender) {
        return;
    }
    atomic_fetch_or_explicit(&sender->state, SENDER_CLOSED_BIT, memory_order_acq_rel);
    uv_mutex_lock(&sender->close_mutex);
    while ((atomic_load_explicit(&sender->state, memory_order_acquire)
            & SENDER_PRODUCER_MASK) != 0) {
        uv_cond_wait(&sender->producers_done, &sender->close_mutex);
    }
    sender->async_handle = NULL;
    uv_mutex_unlock(&sender->close_mutex);
}

struct transfer_s {
    transfer_id_t id;
    CURL *easy;
    hs_easy_data_t *result;
    curl_metrics_context_t *metrics;
    hcurl_stream_t *download_stream;
    hcurl_stream_t *upload_stream;
    bool active;
    transfer_t *next;
};

static size_t hash_transfer_id(transfer_id_t id) {
    uint64_t value = id;
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    value ^= value >> 31;
    return (size_t)value;
}

static bool registry_init(transfer_registry_t *registry) {
    registry->buckets = calloc(INITIAL_BUCKET_COUNT, sizeof(*registry->buckets));
    if (!registry->buckets) {
        registry->bucket_count = 0;
        registry->count = 0;
        return false;
    }
    registry->bucket_count = INITIAL_BUCKET_COUNT;
    registry->count = 0;
    return true;
}

static void registry_destroy(transfer_registry_t *registry) {
    free(registry->buckets);
    registry->buckets = NULL;
    registry->bucket_count = 0;
    registry->count = 0;
}

static transfer_t *registry_find(const transfer_registry_t *registry, transfer_id_t id) {
    if (!registry || !registry->buckets || registry->bucket_count == 0) {
        return NULL;
    }
    size_t bucket = hash_transfer_id(id) & (registry->bucket_count - 1);
    for (transfer_t *transfer = registry->buckets[bucket]; transfer; transfer = transfer->next) {
        if (transfer->id == id) {
            return transfer;
        }
    }
    return NULL;
}

static void registry_resize(transfer_registry_t *registry) {
    if (registry->bucket_count > SIZE_MAX / 2) {
        return;
    }
    size_t new_count = registry->bucket_count * 2;
    transfer_t **new_buckets = calloc(new_count, sizeof(*new_buckets));
    if (!new_buckets) {
        return; /* Chaining remains correct; resizing is only an optimization. */
    }

    for (size_t index = 0; index < registry->bucket_count; ++index) {
        transfer_t *transfer = registry->buckets[index];
        while (transfer) {
            transfer_t *next = transfer->next;
            size_t bucket = hash_transfer_id(transfer->id) & (new_count - 1);
            transfer->next = new_buckets[bucket];
            new_buckets[bucket] = transfer;
            transfer = next;
        }
    }
    free(registry->buckets);
    registry->buckets = new_buckets;
    registry->bucket_count = new_count;
}

static bool registry_insert(transfer_registry_t *registry, transfer_t *transfer) {
    if (!registry || !registry->buckets || !transfer || registry_find(registry, transfer->id)) {
        return false;
    }
    if (registry->bucket_count <= SIZE_MAX / 2
        && registry->count >= registry->bucket_count * 2) {
        registry_resize(registry);
    }
    size_t bucket = hash_transfer_id(transfer->id) & (registry->bucket_count - 1);
    transfer->next = registry->buckets[bucket];
    registry->buckets[bucket] = transfer;
    ++registry->count;
    return true;
}

static transfer_t *registry_remove(transfer_registry_t *registry, transfer_id_t id) {
    if (!registry || !registry->buckets || registry->bucket_count == 0) {
        return NULL;
    }
    size_t bucket = hash_transfer_id(id) & (registry->bucket_count - 1);
    transfer_t **cursor = &registry->buckets[bucket];
    while (*cursor) {
        if ((*cursor)->id == id) {
            transfer_t *removed = *cursor;
            *cursor = removed->next;
            removed->next = NULL;
            --registry->count;
            return removed;
        }
        cursor = &(*cursor)->next;
    }
    return NULL;
}

#if LIBCURL_VERSION_NUM < 0x072000
static CURLMcode drive_legacy_multi(CURLM *multi) {
    int running_handles = 0;
    CURLMcode code;
    do {
        code = curl_multi_socket_all(multi, &running_handles);
    } while (code == CURLM_CALL_MULTI_PERFORM);
    return code;
}
#endif

static void finish_detached_transfer(async_messages_context_t *context,
                                     transfer_t *transfer, CURLcode code) {
    if (!transfer) {
        return;
    }

    if (transfer->active) {
        (void)curl_multi_remove_handle(context->multi, transfer->easy);
        transfer->active = false;
    }
    code = hcurl_stream_normalize_completion(transfer->download_stream, code);
    code = hcurl_stream_normalize_completion(transfer->upload_stream, code);
    curl_metrics_finish(transfer->metrics, transfer->easy);
    hs_easy_data_set_result(transfer->result, transfer->easy, code);
    (void)curl_easy_setopt(transfer->easy, CURLOPT_PRIVATE, NULL);
    hcurl_stream_complete(transfer->download_stream, code);
    hcurl_stream_complete(transfer->upload_stream, code);
    curl_easy_cleanup(transfer->easy);
    hs_easy_data_wake(transfer->result);
    free(transfer);
}

void complete_transfer(async_messages_context_t *context, CURL *easy, CURLcode code) {
    if (!context || !easy) {
        return;
    }
    char *private_data = NULL;
    if (curl_easy_getinfo(easy, CURLINFO_PRIVATE, &private_data) != CURLE_OK
        || !private_data) {
        return;
    }
    transfer_t *transfer = (transfer_t *)private_data;
    transfer_t *registered = registry_remove(&context->registry, transfer->id);
    if (registered != transfer) {
        return;
    }
    finish_detached_transfer(context, transfer, code);
}

static void fail_unregistered_transfer(CURL *easy, hs_easy_data_t *result,
                                       curl_metrics_context_t *metrics,
                                       hcurl_stream_t *download_stream,
                                       hcurl_stream_t *upload_stream,
                                       CURLcode code) {
    curl_metrics_finish(metrics, easy);
    hs_easy_data_set_result(result, easy, code);
    hcurl_stream_complete(download_stream, code);
    hcurl_stream_complete(upload_stream, code);
    if (easy) {
        curl_easy_cleanup(easy);
    }
    hs_easy_data_wake(result);
}

static void execute_transfer(async_messages_context_t *context,
                             const outer_message_execute_payload_t *payload) {
    if (!payload->easy || !payload->result || payload->transfer_id == 0) {
        if (payload->easy || payload->result || payload->download_stream
            || payload->upload_stream) {
            fail_unregistered_transfer(payload->easy, payload->result, payload->metrics,
                                       payload->download_stream, payload->upload_stream,
                                       CURLE_FAILED_INIT);
        }
        return;
    }

    transfer_t *transfer = calloc(1, sizeof(*transfer));
    if (!transfer) {
        fail_unregistered_transfer(payload->easy, payload->result, payload->metrics,
                                   payload->download_stream, payload->upload_stream,
                                   CURLE_OUT_OF_MEMORY);
        return;
    }
    transfer->id = payload->transfer_id;
    transfer->easy = payload->easy;
    transfer->result = payload->result;
    transfer->metrics = payload->metrics;
    transfer->download_stream = payload->download_stream;
    transfer->upload_stream = payload->upload_stream;

    /* A duplicate ID must fail only the new submission.  Removing by ID here
     * would detach the older, still-active transfer from the registry. */
    if (!registry_insert(&context->registry, transfer)) {
        fail_unregistered_transfer(transfer->easy, transfer->result, transfer->metrics,
                                   transfer->download_stream, transfer->upload_stream,
                                   CURLE_FAILED_INIT);
        free(transfer);
        return;
    }

    if (curl_easy_setopt(transfer->easy, CURLOPT_PRIVATE, (void *)transfer) != CURLE_OK) {
        transfer_t *removed = registry_remove(&context->registry, transfer->id);
        (void)removed;
        fail_unregistered_transfer(transfer->easy, transfer->result, transfer->metrics,
                                   transfer->download_stream, transfer->upload_stream,
                                   CURLE_FAILED_INIT);
        free(transfer);
        return;
    }

    if (curl_multi_add_handle(context->multi, transfer->easy) != CURLM_OK) {
        (void)registry_remove(&context->registry, transfer->id);
        finish_detached_transfer(context, transfer, CURLE_FAILED_INIT);
        return;
    }
    transfer->active = true;
#if LIBCURL_VERSION_NUM < 0x071301
    /* curl 7.18 through 7.19.0 can suppress the timer callback when one easy
     * handle is removed and another is added in the same millisecond.  Kick
     * the old socket API here so a newly accepted transfer cannot stall. */
    CURLMcode multi_code = drive_legacy_multi(context->multi);
    if (multi_code != CURLM_OK) {
        transfer = registry_remove(&context->registry, transfer->id);
        finish_detached_transfer(context, transfer, CURLE_RECV_ERROR);
        return;
    }
    check_multi_info(context);
#endif
}

static void cancel_transfer(async_messages_context_t *context, transfer_id_t id) {
    transfer_t *transfer = registry_remove(&context->registry, id);
    if (transfer) {
        finish_detached_transfer(context, transfer, CURLE_ABORTED_BY_CALLBACK);
    }
}

static void resume_transfer(async_messages_context_t *context, transfer_id_t id) {
    transfer_t *transfer = registry_find(&context->registry, id);
    if (!transfer || !transfer->active) {
        return;
    }
    CURLcode code = curl_easy_pause(transfer->easy, CURLPAUSE_CONT);
    if (code != CURLE_OK) {
        transfer = registry_remove(&context->registry, id);
        finish_detached_transfer(context, transfer, code);
        return;
    }
#if LIBCURL_VERSION_NUM < 0x072000
    /* Before 7.32.0 curl_easy_pause() did not schedule the multi handle after
     * an unpause.  In socket mode a fully paused transfer therefore had no
     * fd or timer event capable of making progress.  Drive the old multi API
     * once explicitly; newer libcurl schedules its own immediate timeout. */
    CURLMcode multi_code = drive_legacy_multi(context->multi);
    if (multi_code != CURLM_OK) {
        transfer = registry_remove(&context->registry, id);
        finish_detached_transfer(context, transfer, CURLE_RECV_ERROR);
        return;
    }
#endif
    check_multi_info(context);
}

void destroy_outer_message(outer_message_t *message) {
    free(message);
}

enum enqueue_message_result enqueue_outer_message(
    message_sender_t *sender,
    enum outer_message_types tag,
    transfer_id_t transfer_id,
    CURL *easy,
    hs_easy_data_t *result,
    curl_metrics_context_t *metrics,
    hcurl_stream_t *download_stream,
    hcurl_stream_t *upload_stream) {
    if (!sender) {
        return ENQUEUE_MESSAGE_CLOSED;
    }
    if ((atomic_load_explicit(&sender->state, memory_order_acquire)
         & SENDER_CLOSED_BIT) != 0) {
        return ENQUEUE_MESSAGE_CLOSED;
    }
    outer_message_t *message = calloc(1, sizeof(*message));
    if (!message) {
        return ENQUEUE_MESSAGE_OUT_OF_MEMORY;
    }
    message->tag = tag;
    switch (tag) {
        case EXECUTE:
            message->execute_payload.transfer_id = transfer_id;
            message->execute_payload.easy = easy;
            message->execute_payload.result = result;
            message->execute_payload.metrics = metrics;
            message->execute_payload.download_stream = download_stream;
            message->execute_payload.upload_stream = upload_stream;
            break;
        case CANCEL_REQUEST:
            message->cancel_payload.transfer_id = transfer_id;
            break;
        case RESUME_REQUEST:
            message->resume_payload.transfer_id = transfer_id;
            break;
        case STOP_AGENT:
            break;
    }
    if (!message_sender_enter(sender)) {
        free(message);
        return ENQUEUE_MESSAGE_CLOSED;
    }
    if (!mpscq_enqueue(sender->queue, message)) {
        free(message);
        message_sender_leave(sender);
        return ENQUEUE_MESSAGE_FULL;
    }
    /* Once enqueued, ownership has transferred to the agent.  Even if the
     * loop is already stopping, shutdown_message_context drains the queue. */
    (void)uv_async_send(sender->async_handle);
    message_sender_leave(sender);
    return ENQUEUE_MESSAGE_OK;
}

static void async_check_outer_messages(uv_async_t *async_handle) {
    async_messages_context_t *context = async_handle ? async_handle->data : NULL;
    if (!context) {
        return;
    }

    outer_message_t *message = NULL;
    while ((message = mpscq_dequeue(context->chan))) {
        switch (message->tag) {
            case EXECUTE:
                execute_transfer(context, &message->execute_payload);
                break;
            case CANCEL_REQUEST:
                cancel_transfer(context, message->cancel_payload.transfer_id);
                break;
            case RESUME_REQUEST:
                resume_transfer(context, message->resume_payload.transfer_id);
                break;
            case STOP_AGENT:
                shutdown_message_context(context, CURLE_ABORTED_BY_CALLBACK);
                uv_stop(context->loop);
                break;
        }
        destroy_outer_message(message);
    }
}

uv_async_t *init_async_check_messages(uv_loop_t *loop, mpsc_t *queue, CURLM *multi) {
    if (!loop || !queue || !multi) {
        return NULL;
    }
    uv_async_t *async_handle = malloc(sizeof(*async_handle));
    async_messages_context_t *context = calloc(1, sizeof(*context));
    message_sender_t *sender = message_sender_create(queue);
    if (!async_handle || !context || !sender) {
        free(async_handle);
        free(context);
        message_sender_release(sender);
        return NULL;
    }
    if (!registry_init(&context->registry)) {
        free(async_handle);
        free(context);
        message_sender_release(sender);
        return NULL;
    }

    context->loop = loop;
    context->multi = multi;
    context->chan = queue;
    context->sender = sender;
    async_handle->data = context;
    if (uv_async_init(loop, async_handle, async_check_outer_messages) != 0) {
        registry_destroy(&context->registry);
        message_sender_close(sender);
        message_sender_release(sender);
        free(context);
        free(async_handle);
        return NULL;
    }
    sender->async_handle = async_handle;
    return async_handle;
}

void shutdown_message_context(async_messages_context_t *context, CURLcode code) {
    if (!context) {
        return;
    }
    message_sender_close(context->sender);
    if (!context->registry.buckets) {
        return;
    }
    for (size_t index = 0; index < context->registry.bucket_count; ++index) {
        transfer_t *transfer = context->registry.buckets[index];
        context->registry.buckets[index] = NULL;
        while (transfer) {
            transfer_t *next = transfer->next;
            transfer->next = NULL;
            finish_detached_transfer(context, transfer, code);
            transfer = next;
        }
    }
    context->registry.count = 0;

    /* Execute messages already accepted by the producer side are owned by
     * the agent even if STOP prevented their normal dispatch. */
    outer_message_t *message = NULL;
    while ((message = mpscq_dequeue(context->chan))) {
        if (message->tag == EXECUTE) {
            fail_unregistered_transfer(message->execute_payload.easy,
                                       message->execute_payload.result,
                                       message->execute_payload.metrics,
                                       message->execute_payload.download_stream,
                                       message->execute_payload.upload_stream, code);
        }
        destroy_outer_message(message);
    }
    registry_destroy(&context->registry);
}

void destroy_message_context(async_messages_context_t *context) {
    if (!context) {
        return;
    }
    message_sender_release(context->sender);
    free(context);
}
