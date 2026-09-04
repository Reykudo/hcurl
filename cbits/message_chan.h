#pragma once

#include "curl_metrics.h"
#include "extras.h"
#include "include/waitfree-mpsc-queue/mpscq.h"

#include <HsFFI.h>
#include <curl/curl.h>
#include <stdint.h>
#include <uv.h>

typedef struct mpscq mpsc_t;
typedef uint64_t transfer_id_t;
typedef struct hcurl_stream_s hcurl_stream_t;
typedef struct message_sender_s message_sender_t;

enum outer_message_types {
    EXECUTE,
    CANCEL_REQUEST,
    RESUME_REQUEST,
    STOP_AGENT
};

typedef struct outer_message_execute_payload_s {
    transfer_id_t transfer_id;
    CURL *easy;
    hs_easy_data_t *result;
    curl_metrics_context_t *metrics;
    hcurl_stream_t *download_stream;
    hcurl_stream_t *upload_stream;
} outer_message_execute_payload_t;

typedef struct outer_message_cancel_payload_s {
    transfer_id_t transfer_id;
} outer_message_cancel_payload_t;

typedef struct outer_message_resume_payload_s {
    transfer_id_t transfer_id;
} outer_message_resume_payload_t;

typedef struct outer_message_s {
    enum outer_message_types tag;
    union {
        outer_message_execute_payload_t execute_payload;
        outer_message_cancel_payload_t cancel_payload;
        outer_message_resume_payload_t resume_payload;
    };
} outer_message_t;

typedef struct transfer_s transfer_t;

typedef struct transfer_registry_s {
    transfer_t **buckets;
    size_t bucket_count;
    size_t count;
} transfer_registry_t;

typedef struct async_messages_context_s {
    uv_loop_t *loop;
    CURLM *multi;
    mpsc_t *chan;
    message_sender_t *sender;
    transfer_registry_t registry;
} async_messages_context_t;

uv_async_t *init_async_check_messages(uv_loop_t *loop, mpsc_t *queue, CURLM *multi);

void complete_transfer(async_messages_context_t *context, CURL *easy, CURLcode code);

void shutdown_message_context(async_messages_context_t *context, CURLcode code);

void destroy_message_context(async_messages_context_t *context);

void destroy_outer_message(outer_message_t *message);

/* The sender outlives the uv handle.  Streams retain it, so late close/read
 * operations can observe a closed sender without dereferencing freed libuv
 * state. */
message_sender_t *message_sender_acquire(uv_async_t *async_handle);

bool message_sender_retain(message_sender_t *sender);

void message_sender_release(message_sender_t *sender);

enum enqueue_message_result {
    ENQUEUE_MESSAGE_OK = 0,
    ENQUEUE_MESSAGE_FULL = 1,
    ENQUEUE_MESSAGE_OUT_OF_MEMORY = 2,
    ENQUEUE_MESSAGE_CLOSED = 3
};

enum enqueue_message_result enqueue_outer_message(
    message_sender_t *sender,
    enum outer_message_types tag,
    transfer_id_t transfer_id,
    CURL *easy,
    hs_easy_data_t *result,
    curl_metrics_context_t *metrics,
    hcurl_stream_t *download_stream,
    hcurl_stream_t *upload_stream);
