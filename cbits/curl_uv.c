#include "curl_uv.h"

#include "message_chan.h"

#include <stdlib.h>

static void destroy_socket_context_cb(uv_handle_t *handle) {
    free(handle ? handle->data : NULL);
}

static void destroy_socket_context(socket_context_t *context) {
    if (context && !uv_is_closing((uv_handle_t *)&context->poll_handle)) {
        uv_close((uv_handle_t *)&context->poll_handle, destroy_socket_context_cb);
    }
}

static socket_context_t *new_socket_context(multi_context_t *multi_context,
                                            curl_socket_t socket_fd) {
    socket_context_t *context = calloc(1, sizeof(*context));
    if (!context) {
        return NULL;
    }
    context->multi_context = multi_context;
    context->socket_fd = socket_fd;
    if (uv_poll_init_socket(multi_context->loop, &context->poll_handle, socket_fd) != 0) {
        free(context);
        return NULL;
    }
    context->poll_handle.data = context;
    return context;
}

void check_multi_info(async_messages_context_t *context) {
    if (!context || !context->multi) {
        return;
    }
    int pending = 0;
    CURLMsg *message = NULL;
    while ((message = curl_multi_info_read(context->multi, &pending))) {
        if (message->msg == CURLMSG_DONE) {
            complete_transfer(context, message->easy_handle, message->data.result);
        }
    }
}

static CURLMcode drive_multi_socket(CURLM *multi, curl_socket_t socket_fd,
                                    int flags, int *running_handles) {
    CURLMcode code;
    do {
        code = curl_multi_socket_action(multi, socket_fd, flags,
                                        running_handles);
    } while (code == CURLM_CALL_MULTI_PERFORM);
    return code;
}

static void socket_callback(uv_poll_t *poll, int status, int events) {
    socket_context_t *socket_context = poll ? poll->data : NULL;
    if (!socket_context || !socket_context->multi_context) {
        return;
    }
    multi_context_t *multi_context = socket_context->multi_context;
    int flags = 0;
    if (status < 0) {
        flags = CURL_CSELECT_ERR;
    } else {
        if (events & UV_READABLE) {
            flags |= CURL_CSELECT_IN;
        }
        if (events & UV_WRITABLE) {
            flags |= CURL_CSELECT_OUT;
        }
    }

    int running_handles = 0;
    CURLMcode code = drive_multi_socket(multi_context->multi,
                                        socket_context->socket_fd,
                                        flags, &running_handles);
    if (code == CURLM_BAD_SOCKET) {
        /* Old libcurl releases a socket before an already queued libuv event
         * for that fd is dispatched.  CURLM_BAD_SOCKET is therefore a stale
         * notification, not a failure of the multi handle.  Retire our poll
         * watcher and keep the reactor alive for the remaining transfers. */
        if (!uv_is_closing((uv_handle_t *)poll)) {
            (void)uv_poll_stop(poll);
            destroy_socket_context(socket_context);
        }
#if LIBCURL_VERSION_NUM < 0x071301
        do {
            code = curl_multi_socket_all(multi_context->multi, &running_handles);
        } while (code == CURLM_CALL_MULTI_PERFORM);
        if (code != CURLM_OK) {
            shutdown_message_context(multi_context->messages, CURLE_RECV_ERROR);
            uv_stop(multi_context->loop);
            return;
        }
#endif
        check_multi_info(multi_context->messages);
        return;
    }
    if (code != CURLM_OK) {
        shutdown_message_context(multi_context->messages, CURLE_RECV_ERROR);
        uv_stop(multi_context->loop);
        return;
    }
    check_multi_info(multi_context->messages);
}

static void on_timeout(uv_timer_t *timer) {
    multi_context_t *multi_context = timer ? timer->data : NULL;
    if (!multi_context) {
        return;
    }
    int running_handles = 0;
    CURLMcode code = drive_multi_socket(multi_context->multi,
                                        CURL_SOCKET_TIMEOUT, 0,
                                        &running_handles);
    if (code != CURLM_OK) {
        shutdown_message_context(multi_context->messages, CURLE_OPERATION_TIMEDOUT);
        uv_stop(multi_context->loop);
        return;
    }
    check_multi_info(multi_context->messages);
}

static int curl_timer_function(CURLM *multi, long timeout_ms, void *userdata) {
    (void)multi;
    multi_context_t *context = userdata;
    if (!context) {
        return -1;
    }
    if (timeout_ms < 0) {
        return uv_timer_stop(&context->timer) == 0 ? 0 : -1;
    }
    /* A zero timeout is deliberately passed through: libuv schedules it for
     * the next loop iteration, so there is no recursive curl call and no
     * artificial one-millisecond startup delay. */
    return uv_timer_start(&context->timer, on_timeout, (uint64_t)timeout_ms, 0) == 0
               ? 0
               : -1;
}

static int curl_socket_function(CURL *easy, curl_socket_t socket_fd, int action,
                                void *userdata, void *socket_data) {
    (void)easy;
    multi_context_t *multi_context = userdata;
    socket_context_t *context = socket_data;
    if (!multi_context) {
        return -1;
    }

    switch (action) {
        case CURL_POLL_IN:
        case CURL_POLL_OUT:
        case CURL_POLL_INOUT: {
            if (!context || uv_is_closing((uv_handle_t *)&context->poll_handle)) {
                context = new_socket_context(multi_context, socket_fd);
                if (!context
                    || curl_multi_assign(multi_context->multi, socket_fd, context) != CURLM_OK) {
                    destroy_socket_context(context);
                    return -1;
                }
            }
            int events = 0;
            if (action != CURL_POLL_IN) {
                events |= UV_WRITABLE;
            }
            if (action != CURL_POLL_OUT) {
                events |= UV_READABLE;
            }
            return uv_poll_start(&context->poll_handle, events, socket_callback) == 0 ? 0 : -1;
        }
        case CURL_POLL_REMOVE:
            if (context) {
                (void)uv_poll_stop(&context->poll_handle);
                (void)curl_multi_assign(multi_context->multi, socket_fd, NULL);
                destroy_socket_context(context);
            }
            return 0;
        default:
            return -1;
    }
}

bool bind_uv_curl_multi(uv_loop_t *loop, CURLM *multi,
                        async_messages_context_t *messages) {
    if (!loop || !multi || !messages) {
        return false;
    }
    multi_context_t *context = calloc(1, sizeof(*context));
    if (!context) {
        return false;
    }
    context->loop = loop;
    context->multi = multi;
    context->messages = messages;

    if (curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, curl_socket_function) != CURLM_OK
        || curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, context) != CURLM_OK
        || curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, curl_timer_function) != CURLM_OK
        || curl_multi_setopt(multi, CURLMOPT_TIMERDATA, context) != CURLM_OK) {
        (void)curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_TIMERDATA, NULL);
        free(context);
        return false;
    }
    if (uv_timer_init(loop, &context->timer) != 0) {
        (void)curl_multi_setopt(multi, CURLMOPT_SOCKETFUNCTION, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_SOCKETDATA, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_TIMERFUNCTION, NULL);
        (void)curl_multi_setopt(multi, CURLMOPT_TIMERDATA, NULL);
        free(context);
        return false;
    }
    context->timer.data = context;
    loop->data = context;
    return true;
}

static void close_handle_cb(uv_handle_t *handle, void *arg) {
    (void)arg;
    if (!uv_is_closing(handle)) {
        if (handle->type == UV_POLL) {
            uv_close(handle, destroy_socket_context_cb);
        } else {
            uv_close(handle, NULL);
        }
    }
}

void agent_shutdown(uv_loop_t *loop, uv_async_t *async_handle, CURLM *multi) {
    if (!loop) {
        return;
    }
    async_messages_context_t *messages = async_handle ? async_handle->data : NULL;
    /* libuv 1.0 poisons the uv_loop_t storage in debug builds when
     * uv_loop_close succeeds, including loop->data.  Keep the allocation we
     * own before closing the loop instead of reading poisoned memory. */
    multi_context_t *multi_context = loop->data;
    shutdown_message_context(messages, CURLE_ABORTED_BY_CALLBACK);

    if (multi) {
        (void)curl_multi_cleanup(multi);
    }
    uv_walk(loop, close_handle_cb, NULL);
    while (uv_loop_alive(loop)) {
        (void)uv_run(loop, UV_RUN_NOWAIT);
    }
    (void)uv_loop_close(loop);

    free(multi_context);
    if (async_handle) {
        destroy_message_context(async_handle->data);
    }
    free(async_handle);
    free(loop);
}
