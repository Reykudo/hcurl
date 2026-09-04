#pragma once

#include <curl/curl.h>
#include <stdbool.h>
#include <uv.h>

typedef struct async_messages_context_s async_messages_context_t;

typedef struct multi_context_s {
    uv_loop_t *loop;
    CURLM *multi;
    uv_timer_t timer;
    async_messages_context_t *messages;
} multi_context_t;

typedef struct socket_context_s {
    uv_poll_t poll_handle;
    curl_socket_t socket_fd;
    multi_context_t *multi_context;
} socket_context_t;

void check_multi_info(async_messages_context_t *context);

bool bind_uv_curl_multi(uv_loop_t *loop, CURLM *multi,
                        async_messages_context_t *messages);

/* Tear down an agent loop after uv_run returned.  Any active or accepted
 * transfers are completed before their easy handles are destroyed. */
void agent_shutdown(uv_loop_t *loop, uv_async_t *async_handle, CURLM *multi);
