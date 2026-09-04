#pragma once

#include "message_chan.h"

#include <HsFFI.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <stddef.h>
#include <stdint.h>
#include <uv.h>

typedef struct hcurl_stream_s hcurl_stream_t;
typedef struct header_data_s header_data_t;

enum hcurl_stream_kind {
    HCURL_STREAM_DOWNLOAD = 0,
    HCURL_STREAM_UPLOAD = 1
};

enum hcurl_stream_read_result {
    HCURL_STREAM_READ_CHUNK = 0,
    HCURL_STREAM_READ_WOULD_BLOCK = 1,
    HCURL_STREAM_READ_EOF = 2,
    HCURL_STREAM_READ_ERROR = 3,
    HCURL_STREAM_READ_CLOSED = 4,
    HCURL_STREAM_READ_CHUNK_CONTROL_PENDING = 5
};

enum hcurl_stream_wait_result {
    HCURL_STREAM_WAIT_REGISTERED = 0,
    HCURL_STREAM_WAIT_READY = 1,
    HCURL_STREAM_WAIT_CONTROL_PENDING = 2,
    HCURL_STREAM_WAIT_INVALID = 3
};

enum hcurl_stream_write_result {
    HCURL_STREAM_WRITE_OK = 0,
    HCURL_STREAM_WRITE_WOULD_BLOCK = 1,
    HCURL_STREAM_WRITE_TERMINAL = 2,
    HCURL_STREAM_WRITE_CLOSED = 3,
    HCURL_STREAM_WRITE_CONTROL_PENDING = 4,
    HCURL_STREAM_WRITE_OUT_OF_MEMORY = 5
};

enum hcurl_stream_control_result {
    HCURL_STREAM_CONTROL_OK = 0,
    HCURL_STREAM_CONTROL_RETRY = 1,
    HCURL_STREAM_CONTROL_CLOSED = 2
};

#define HCURL_STREAM_UPLOAD_UNSUPPORTED (-1)

hcurl_stream_t *hcurl_stream_create(enum hcurl_stream_kind kind,
                                    size_t capacity,
                                    message_sender_t *sender,
                                    transfer_id_t transfer_id);

void hcurl_stream_destroy(hcurl_stream_t *stream);

CURLcode hcurl_stream_install_download(CURL *easy, hcurl_stream_t *stream,
                                       header_data_t *headers,
                                       curl_metrics_context_t *metrics);

/* Returns a CURLcode value, or HCURL_STREAM_UPLOAD_UNSUPPORTED when the
 * linked libcurl has the unsafe 7.18 upload-pause implementation. */
int hcurl_stream_install_upload(CURL *easy, hcurl_stream_t *stream,
                                curl_metrics_context_t *metrics);

bool hcurl_stream_upload_supported(void);

size_t hcurl_download_write_callback(char *data, size_t size, size_t count,
                                     void *userdata);

size_t hcurl_upload_read_callback(char *buffer, size_t size, size_t count,
                                  void *userdata);

enum hcurl_stream_read_result hcurl_stream_try_read(hcurl_stream_t *stream,
                                                     void **data,
                                                     size_t *length,
                                                     int *terminal_code);

enum hcurl_stream_wait_result hcurl_stream_wait_read(hcurl_stream_t *stream,
                                                      HsStablePtr mvar,
                                                      int capability);

bool hcurl_stream_cancel_wait_read(hcurl_stream_t *stream, HsStablePtr mvar);

enum hcurl_stream_write_result hcurl_stream_try_write(hcurl_stream_t *stream,
                                                       const void *data,
                                                       size_t length,
                                                       int *terminal_code);

enum hcurl_stream_wait_result hcurl_stream_wait_write(hcurl_stream_t *stream,
                                                       HsStablePtr mvar,
                                                       int capability);

bool hcurl_stream_cancel_wait_write(hcurl_stream_t *stream, HsStablePtr mvar);

enum hcurl_stream_write_result hcurl_stream_finish_upload(
    hcurl_stream_t *stream, int *terminal_code);

enum hcurl_stream_control_result hcurl_stream_close(hcurl_stream_t *stream);

enum hcurl_stream_control_result hcurl_stream_flush_resume(hcurl_stream_t *stream);

CURLcode hcurl_stream_normalize_completion(hcurl_stream_t *stream,
                                           CURLcode terminal_code);

void hcurl_stream_complete(hcurl_stream_t *stream, CURLcode terminal_code);
