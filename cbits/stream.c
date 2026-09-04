#include "stream.h"

#include "extras.h"
#include "headers.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct hcurl_chunk_s {
    unsigned char *data;
    size_t length;
    size_t offset;
} hcurl_chunk_t;

struct hcurl_stream_s {
    uv_mutex_t mutex;
    enum hcurl_stream_kind kind;
    hcurl_chunk_t *chunks;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    bool finished;
    bool closed;
    bool paused;
    bool terminal_set;
    bool cancel_sent;
    CURLcode terminal_code;
    bool read_waiter_present;
    hs_waker_t read_waiter;
    bool write_waiter_present;
    hs_waker_t write_waiter;
    message_sender_t *sender;
    transfer_id_t transfer_id;
    CURL *easy;
    header_data_t *headers;
    curl_metrics_context_t *metrics;
};

static void clear_chunk(hcurl_chunk_t *chunk) {
    if (!chunk) {
        return;
    }
    free(chunk->data);
    chunk->data = NULL;
    chunk->length = 0;
    chunk->offset = 0;
}

CURLcode hcurl_stream_install_download(CURL *easy, hcurl_stream_t *stream,
                                       header_data_t *headers,
                                       curl_metrics_context_t *metrics) {
    if (!easy || !stream || stream->kind != HCURL_STREAM_DOWNLOAD
        || !headers || !metrics) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    uv_mutex_lock(&stream->mutex);
    stream->easy = easy;
    stream->headers = headers;
    stream->metrics = metrics;
    uv_mutex_unlock(&stream->mutex);
    curl_metrics_streamed_download(metrics);
    CURLcode code = curl_easy_setopt(easy, CURLOPT_WRITEDATA, stream);
    if (code != CURLE_OK) return code;
    return curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, hcurl_download_write_callback);
}

int hcurl_stream_install_upload(CURL *easy, hcurl_stream_t *stream,
                                curl_metrics_context_t *metrics) {
    if (!easy || !stream || stream->kind != HCURL_STREAM_UPLOAD || !metrics) {
        return (int)CURLE_BAD_FUNCTION_ARGUMENT;
    }
    /* CURL_READFUNC_PAUSE exists in 7.18.x, but those releases apply it to
     * the receive direction and can send an uninitialized byte count.  There
     * is no safe event-driven workaround outside libcurl. */
    if (!hcurl_stream_upload_supported()) {
        return HCURL_STREAM_UPLOAD_UNSUPPORTED;
    }
    uv_mutex_lock(&stream->mutex);
    stream->easy = easy;
    stream->metrics = metrics;
    uv_mutex_unlock(&stream->mutex);
    curl_metrics_streamed_upload(metrics);
    CURLcode code = curl_easy_setopt(easy, CURLOPT_READFUNCTION, hcurl_upload_read_callback);
    if (code != CURLE_OK) return (int)code;
    return (int)curl_easy_setopt(easy, CURLOPT_READDATA, stream);
}

bool hcurl_stream_upload_supported(void) {
    /* The public capability probe may be called before an Agent exists.
     * Serializing global initialization here keeps curl_version_info safe on
     * old libcurl releases where pre-init concurrent calls were not safe. */
    if (hcurl_global_init_once() != CURLE_OK) {
        return false;
    }
    const curl_version_info_data *runtime = curl_version_info(CURLVERSION_NOW);
    return runtime && runtime->version_num >= 0x071300;
}

static void drop_chunks_locked(hcurl_stream_t *stream) {
    while (stream->count > 0) {
        clear_chunk(&stream->chunks[stream->head]);
        stream->head = (stream->head + 1) % stream->capacity;
        --stream->count;
    }
    stream->tail = stream->head;
}

static bool detach_waiter_locked(bool *present, hs_waker_t *stored,
                                 hs_waker_t *detached) {
    if (!*present) {
        return false;
    }
    *detached = *stored;
    *present = false;
    stored->mvar = NULL;
    return true;
}

static void wake_detached(bool present, hs_waker_t *waker) {
    if (present) {
        wake_up_waker(waker);
    }
}

hcurl_stream_t *hcurl_stream_create(enum hcurl_stream_kind kind,
                                    size_t capacity,
                                    message_sender_t *sender,
                                    transfer_id_t transfer_id) {
    if (capacity == 0 || capacity > SIZE_MAX / sizeof(hcurl_chunk_t)
        || !sender || transfer_id == 0 || !message_sender_retain(sender)) {
        return NULL;
    }
    hcurl_stream_t *stream = calloc(1, sizeof(*stream));
    if (!stream) {
        message_sender_release(sender);
        return NULL;
    }
    stream->chunks = calloc(capacity, sizeof(*stream->chunks));
    if (!stream->chunks || uv_mutex_init(&stream->mutex) != 0) {
        free(stream->chunks);
        free(stream);
        message_sender_release(sender);
        return NULL;
    }
    stream->kind = kind;
    stream->capacity = capacity;
    stream->terminal_code = CURLE_OK;
    stream->sender = sender;
    stream->transfer_id = transfer_id;
    return stream;
}

void hcurl_stream_destroy(hcurl_stream_t *stream) {
    if (!stream) {
        return;
    }
    hs_waker_t read_waiter = {0};
    hs_waker_t write_waiter = {0};
    bool wake_read = false;
    bool wake_write = false;
    uv_mutex_lock(&stream->mutex);
    stream->closed = true;
    drop_chunks_locked(stream);
    wake_read = detach_waiter_locked(&stream->read_waiter_present,
                                     &stream->read_waiter, &read_waiter);
    wake_write = detach_waiter_locked(&stream->write_waiter_present,
                                      &stream->write_waiter, &write_waiter);
    uv_mutex_unlock(&stream->mutex);
    wake_detached(wake_read, &read_waiter);
    wake_detached(wake_write, &write_waiter);
    uv_mutex_destroy(&stream->mutex);
    free(stream->chunks);
    message_sender_release(stream->sender);
    free(stream);
}

static bool checked_transfer_size(size_t size, size_t count, size_t *total) {
    if (size != 0 && count > SIZE_MAX / size) {
        return false;
    }
    *total = size * count;
    return true;
}

size_t hcurl_download_write_callback(char *data, size_t size, size_t count,
                                     void *userdata) {
    hcurl_stream_t *stream = userdata;
    size_t total = 0;
    if (!stream || stream->kind != HCURL_STREAM_DOWNLOAD
        || !checked_transfer_size(size, count, &total)) {
        return 0;
    }
    if (total == 0) {
        return 0;
    }

    header_data_mark_body_started(stream->headers);
    hs_waker_t waiter = {0};
    bool wake_reader = false;
    CURL *easy_to_pause = NULL;
    uv_mutex_lock(&stream->mutex);
    if (stream->closed || stream->terminal_set) {
        uv_mutex_unlock(&stream->mutex);
        return 0;
    }
    if (stream->count == stream->capacity) {
        stream->paused = true;
        uv_mutex_unlock(&stream->mutex);
        return CURL_WRITEFUNC_PAUSE;
    }

    unsigned char *copy = malloc(total);
    if (!copy) {
        uv_mutex_unlock(&stream->mutex);
        return 0;
    }
    memcpy(copy, data, total);
    hcurl_chunk_t *chunk = &stream->chunks[stream->tail];
    chunk->data = copy;
    chunk->length = total;
    chunk->offset = 0;
    stream->tail = (stream->tail + 1) % stream->capacity;
    ++stream->count;
    curl_metrics_add_downloaded(stream->metrics, total);
    if (stream->count == stream->capacity) {
        /* Pause before returning the accepted chunk.  Returning
         * CURL_WRITEFUNC_PAUSE makes libcurl retain and replay this chunk;
         * libcurl 7.18.0 loses all but CURL_MAX_WRITE_SIZE bytes from a
         * retained block when it is resumed.  Explicitly pausing here keeps
         * buffering entirely in our bounded ring and works around that
         * first-release bug without a version check. */
        stream->paused = true;
        easy_to_pause = stream->easy;
    }
    wake_reader = detach_waiter_locked(&stream->read_waiter_present,
                                        &stream->read_waiter, &waiter);
    uv_mutex_unlock(&stream->mutex);
    wake_detached(wake_reader, &waiter);
    if (easy_to_pause
        && curl_easy_pause(easy_to_pause, CURLPAUSE_RECV) != CURLE_OK) {
        return 0;
    }
    return total;
}

static enum hcurl_stream_control_result enqueue_control(hcurl_stream_t *stream,
                                                         enum outer_message_types tag) {
    enum enqueue_message_result result =
        enqueue_outer_message(stream->sender, tag,
                              stream->transfer_id, NULL, NULL, NULL, NULL, NULL);
    switch (result) {
        case ENQUEUE_MESSAGE_OK:
            return HCURL_STREAM_CONTROL_OK;
        case ENQUEUE_MESSAGE_CLOSED:
            return HCURL_STREAM_CONTROL_CLOSED;
        case ENQUEUE_MESSAGE_FULL:
        case ENQUEUE_MESSAGE_OUT_OF_MEMORY:
            return HCURL_STREAM_CONTROL_RETRY;
    }
    return HCURL_STREAM_CONTROL_RETRY;
}

enum hcurl_stream_control_result hcurl_stream_flush_resume(hcurl_stream_t *stream) {
    if (!stream) {
        return HCURL_STREAM_CONTROL_OK;
    }
    uv_mutex_lock(&stream->mutex);
    bool needs_resume = stream->paused && !stream->closed
        && !stream->terminal_set && !stream->cancel_sent;
    if (needs_resume) {
        /* Clear before enqueue: the agent may process RESUME immediately and
         * synchronously pause again from the curl callback.  Clearing after
         * enqueue would overwrite that newer pause and lose a wake-up. */
        stream->paused = false;
    }
    uv_mutex_unlock(&stream->mutex);
    if (!needs_resume) {
        return HCURL_STREAM_CONTROL_OK;
    }
    enum hcurl_stream_control_result result = enqueue_control(stream, RESUME_REQUEST);
    if (result == HCURL_STREAM_CONTROL_CLOSED) {
        /* The reactor is already shutting down and can no longer resume this
         * transfer.  Publish a local terminal state so a late reader/writer
         * cannot spin forever retrying against a closed sender. */
        hcurl_stream_complete(stream, CURLE_ABORTED_BY_CALLBACK);
        return HCURL_STREAM_CONTROL_OK;
    }
    if (result == HCURL_STREAM_CONTROL_RETRY) {
        uv_mutex_lock(&stream->mutex);
        if (!stream->closed && !stream->terminal_set && !stream->cancel_sent) {
            stream->paused = true;
        }
        uv_mutex_unlock(&stream->mutex);
    }
    return result;
}

enum hcurl_stream_read_result hcurl_stream_try_read(hcurl_stream_t *stream,
                                                     void **data,
                                                     size_t *length,
                                                     int *terminal_code) {
    if (!stream || stream->kind != HCURL_STREAM_DOWNLOAD || !data || !length
        || !terminal_code) {
        return HCURL_STREAM_READ_CLOSED;
    }
    *data = NULL;
    *length = 0;
    *terminal_code = (int)CURLE_OK;

    bool should_resume = false;
    uv_mutex_lock(&stream->mutex);
    if (stream->count > 0) {
        hcurl_chunk_t *chunk = &stream->chunks[stream->head];
        *data = chunk->data;
        *length = chunk->length;
        chunk->data = NULL;
        chunk->length = 0;
        chunk->offset = 0;
        stream->head = (stream->head + 1) % stream->capacity;
        --stream->count;
        should_resume = stream->paused;
        uv_mutex_unlock(&stream->mutex);
        if (should_resume
            && hcurl_stream_flush_resume(stream) != HCURL_STREAM_CONTROL_OK) {
            return HCURL_STREAM_READ_CHUNK_CONTROL_PENDING;
        }
        return HCURL_STREAM_READ_CHUNK;
    }
    if (stream->closed) {
        *terminal_code = (int)CURLE_ABORTED_BY_CALLBACK;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_READ_CLOSED;
    }
    if (stream->terminal_set) {
        *terminal_code = (int)stream->terminal_code;
        enum hcurl_stream_read_result result =
            stream->terminal_code == CURLE_OK ? HCURL_STREAM_READ_EOF
                                               : HCURL_STREAM_READ_ERROR;
        uv_mutex_unlock(&stream->mutex);
        return result;
    }
    uv_mutex_unlock(&stream->mutex);
    return HCURL_STREAM_READ_WOULD_BLOCK;
}

enum hcurl_stream_wait_result hcurl_stream_wait_read(hcurl_stream_t *stream,
                                                      HsStablePtr mvar,
                                                      int capability) {
    if (!stream || !mvar || stream->kind != HCURL_STREAM_DOWNLOAD) {
        return HCURL_STREAM_WAIT_INVALID;
    }
    uv_mutex_lock(&stream->mutex);
    if (stream->count > 0 || stream->closed || stream->terminal_set) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WAIT_READY;
    }
    if (stream->paused) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WAIT_CONTROL_PENDING;
    }
    if (stream->read_waiter_present) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WAIT_INVALID;
    }
    stream->read_waiter.mvar = mvar;
    stream->read_waiter.capability = capability;
    stream->read_waiter.waked = false;
    stream->read_waiter_present = true;
    uv_mutex_unlock(&stream->mutex);
    return HCURL_STREAM_WAIT_REGISTERED;
}

static bool cancel_waiter(hcurl_stream_t *stream, bool *present,
                          hs_waker_t *waiter, HsStablePtr mvar) {
    if (!stream || !mvar) {
        return false;
    }
    bool removed = false;
    uv_mutex_lock(&stream->mutex);
    if (*present && waiter->mvar == mvar) {
        *present = false;
        waiter->mvar = NULL;
        removed = true;
    }
    uv_mutex_unlock(&stream->mutex);
    return removed;
}

bool hcurl_stream_cancel_wait_read(hcurl_stream_t *stream, HsStablePtr mvar) {
    if (!stream || stream->kind != HCURL_STREAM_DOWNLOAD) {
        return false;
    }
    return cancel_waiter(stream, &stream->read_waiter_present,
                         &stream->read_waiter, mvar);
}

size_t hcurl_upload_read_callback(char *buffer, size_t size, size_t count,
                                  void *userdata) {
    hcurl_stream_t *stream = userdata;
    size_t capacity = 0;
    if (!stream || stream->kind != HCURL_STREAM_UPLOAD
        || !checked_transfer_size(size, count, &capacity)) {
        return CURL_READFUNC_ABORT;
    }
    if (capacity == 0) {
        return 0;
    }

    hs_waker_t waiter = {0};
    bool wake_writer = false;
    uv_mutex_lock(&stream->mutex);
    if (stream->closed || (stream->terminal_set && stream->terminal_code != CURLE_OK)) {
        uv_mutex_unlock(&stream->mutex);
        return CURL_READFUNC_ABORT;
    }
    if (stream->count == 0) {
        if (stream->finished) {
            uv_mutex_unlock(&stream->mutex);
            return 0;
        }
        stream->paused = true;
        uv_mutex_unlock(&stream->mutex);
        return CURL_READFUNC_PAUSE;
    }

    bool was_full = stream->count == stream->capacity;
    hcurl_chunk_t *chunk = &stream->chunks[stream->head];
    size_t remaining = chunk->length - chunk->offset;
    size_t copied = remaining < capacity ? remaining : capacity;
    memcpy(buffer, chunk->data + chunk->offset, copied);
    chunk->offset += copied;
    if (chunk->offset == chunk->length) {
        clear_chunk(chunk);
        stream->head = (stream->head + 1) % stream->capacity;
        --stream->count;
    }
    curl_metrics_add_uploaded(stream->metrics, copied);
    if (was_full && stream->count < stream->capacity) {
        wake_writer = detach_waiter_locked(&stream->write_waiter_present,
                                            &stream->write_waiter, &waiter);
    }
    uv_mutex_unlock(&stream->mutex);
    wake_detached(wake_writer, &waiter);
    return copied;
}

enum hcurl_stream_write_result hcurl_stream_try_write(hcurl_stream_t *stream,
                                                       const void *data,
                                                       size_t length,
                                                       int *terminal_code) {
    if (!stream || stream->kind != HCURL_STREAM_UPLOAD || !terminal_code
        || (length > 0 && !data)) {
        return HCURL_STREAM_WRITE_CLOSED;
    }
    *terminal_code = (int)CURLE_OK;

    uv_mutex_lock(&stream->mutex);
    if (stream->closed) {
        *terminal_code = (int)CURLE_ABORTED_BY_CALLBACK;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_CLOSED;
    }
    if (stream->terminal_set) {
        *terminal_code = (int)stream->terminal_code;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_TERMINAL;
    }
    if (stream->finished) {
        *terminal_code = (int)CURLE_ABORTED_BY_CALLBACK;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_CLOSED;
    }
    if (length == 0) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_OK;
    }
    if (stream->count == stream->capacity) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_WOULD_BLOCK;
    }

    unsigned char *copy = malloc(length);
    if (!copy) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_OUT_OF_MEMORY;
    }
    memcpy(copy, data, length);
    hcurl_chunk_t *chunk = &stream->chunks[stream->tail];
    chunk->data = copy;
    chunk->length = length;
    chunk->offset = 0;
    stream->tail = (stream->tail + 1) % stream->capacity;
    ++stream->count;
    bool needs_resume = stream->paused;
    uv_mutex_unlock(&stream->mutex);

    if (needs_resume && hcurl_stream_flush_resume(stream) != HCURL_STREAM_CONTROL_OK) {
        return HCURL_STREAM_WRITE_CONTROL_PENDING;
    }
    return HCURL_STREAM_WRITE_OK;
}

enum hcurl_stream_wait_result hcurl_stream_wait_write(hcurl_stream_t *stream,
                                                       HsStablePtr mvar,
                                                       int capability) {
    if (!stream || !mvar || stream->kind != HCURL_STREAM_UPLOAD) {
        return HCURL_STREAM_WAIT_INVALID;
    }
    uv_mutex_lock(&stream->mutex);
    if (stream->count < stream->capacity || stream->closed || stream->terminal_set) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WAIT_READY;
    }
    if (stream->write_waiter_present) {
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WAIT_INVALID;
    }
    stream->write_waiter.mvar = mvar;
    stream->write_waiter.capability = capability;
    stream->write_waiter.waked = false;
    stream->write_waiter_present = true;
    uv_mutex_unlock(&stream->mutex);
    return HCURL_STREAM_WAIT_REGISTERED;
}

bool hcurl_stream_cancel_wait_write(hcurl_stream_t *stream, HsStablePtr mvar) {
    if (!stream || stream->kind != HCURL_STREAM_UPLOAD) {
        return false;
    }
    return cancel_waiter(stream, &stream->write_waiter_present,
                         &stream->write_waiter, mvar);
}

enum hcurl_stream_write_result hcurl_stream_finish_upload(
    hcurl_stream_t *stream, int *terminal_code) {
    if (!stream || stream->kind != HCURL_STREAM_UPLOAD || !terminal_code) {
        return HCURL_STREAM_WRITE_CLOSED;
    }
    *terminal_code = (int)CURLE_OK;
    uv_mutex_lock(&stream->mutex);
    if (stream->closed) {
        *terminal_code = (int)CURLE_ABORTED_BY_CALLBACK;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_CLOSED;
    }
    if (stream->terminal_set) {
        *terminal_code = (int)stream->terminal_code;
        uv_mutex_unlock(&stream->mutex);
        return HCURL_STREAM_WRITE_TERMINAL;
    }
    stream->finished = true;
    bool needs_resume = stream->paused;
    uv_mutex_unlock(&stream->mutex);
    if (needs_resume && hcurl_stream_flush_resume(stream) != HCURL_STREAM_CONTROL_OK) {
        return HCURL_STREAM_WRITE_CONTROL_PENDING;
    }
    return HCURL_STREAM_WRITE_OK;
}

enum hcurl_stream_control_result hcurl_stream_close(hcurl_stream_t *stream) {
    if (!stream) {
        return HCURL_STREAM_CONTROL_OK;
    }
    hs_waker_t read_waiter = {0};
    hs_waker_t write_waiter = {0};
    bool wake_read = false;
    bool wake_write = false;
    uv_mutex_lock(&stream->mutex);
    stream->closed = true;
    stream->paused = false;
    drop_chunks_locked(stream);
    wake_read = detach_waiter_locked(&stream->read_waiter_present,
                                     &stream->read_waiter, &read_waiter);
    wake_write = detach_waiter_locked(&stream->write_waiter_present,
                                      &stream->write_waiter, &write_waiter);
    bool needs_cancel = !stream->terminal_set && !stream->cancel_sent;
    if (needs_cancel) {
        stream->cancel_sent = true;
    }
    uv_mutex_unlock(&stream->mutex);
    wake_detached(wake_read, &read_waiter);
    wake_detached(wake_write, &write_waiter);

    if (!needs_cancel) {
        return HCURL_STREAM_CONTROL_OK;
    }
    enum hcurl_stream_control_result result = enqueue_control(stream, CANCEL_REQUEST);
    if (result == HCURL_STREAM_CONTROL_CLOSED) {
        hcurl_stream_complete(stream, CURLE_ABORTED_BY_CALLBACK);
        return HCURL_STREAM_CONTROL_OK;
    }
    if (result == HCURL_STREAM_CONTROL_RETRY) {
        uv_mutex_lock(&stream->mutex);
        if (!stream->terminal_set) {
            stream->cancel_sent = false;
        }
        uv_mutex_unlock(&stream->mutex);
    }
    return result;
}

CURLcode hcurl_stream_normalize_completion(hcurl_stream_t *stream,
                                           CURLcode terminal_code) {
    if (!stream) {
        return terminal_code;
    }
    uv_mutex_lock(&stream->mutex);
    bool cancelled = stream->closed || stream->cancel_sent;
    uv_mutex_unlock(&stream->mutex);
    return cancelled ? CURLE_ABORTED_BY_CALLBACK : terminal_code;
}

void hcurl_stream_complete(hcurl_stream_t *stream, CURLcode terminal_code) {
    if (!stream) {
        return;
    }
    header_data_mark_complete(stream->headers, terminal_code);
    hs_waker_t read_waiter = {0};
    hs_waker_t write_waiter = {0};
    bool wake_read = false;
    bool wake_write = false;
    uv_mutex_lock(&stream->mutex);
    if (!stream->terminal_set) {
        stream->terminal_set = true;
        stream->terminal_code = terminal_code;
    }
    stream->easy = NULL;
    stream->paused = false;
    wake_read = detach_waiter_locked(&stream->read_waiter_present,
                                     &stream->read_waiter, &read_waiter);
    wake_write = detach_waiter_locked(&stream->write_waiter_present,
                                      &stream->write_waiter, &write_waiter);
    uv_mutex_unlock(&stream->mutex);
    wake_detached(wake_read, &read_waiter);
    wake_detached(wake_write, &write_waiter);
}
