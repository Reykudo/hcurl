#include "message_chan.h"
#include <uv.h>
#include <curl/curl.h>
#include <stdlib.h>
#include <assert.h>
#include "extras.h"

void check_multi_info(CURLM *multi);

void destroy_outer_message(outer_message_t *message) {
    if (!message) {
        return;
    }

    if (message->tag == CANCEL_REQUEST
        && message->cancel_payload.waker.mvar
        && !message->cancel_payload.waker.waked) {
        hs_free_stable_ptr(message->cancel_payload.waker.mvar);
        message->cancel_payload.waker.mvar = NULL;
    }
    free(message);
}

void async_check_outer_messages(uv_async_t *async_handle) {
    async_messages_context_t *context = async_handle->data;

    assert(context);
    assert(context->multi);
    assert(context->chan);

    outer_message_t *message = NULL;

    while ((message = mpscq_dequeue(context->chan))) {
        assert(message);
        switch (message->tag) {
            case EXECUTE: {
                assert(message->execute_payload.easy);
                hs_easy_data_t *execute_data = NULL;
                curl_easy_getinfo(message->execute_payload.easy, CURLINFO_PRIVATE, &execute_data);
                if (curl_multi_add_handle(context->multi, message->execute_payload.easy) == CURLM_OK) {
                    execute_data->active = true;
                } else {
                    execute_data->curl_code = CURLE_FAILED_INIT;
                    wake_up_waker(&execute_data->waker);
                }
                break;
            }
            case CANCEL_REQUEST: {
                assert(message->cancel_payload.easy);
                assert(message->cancel_payload.waker.mvar);
                hs_easy_data_t *cancel_data = NULL;
                curl_easy_getinfo(message->cancel_payload.easy, CURLINFO_PRIVATE, &cancel_data);
                if (cancel_data->active) {
                    curl_multi_remove_handle(context->multi, message->cancel_payload.easy);
                    cancel_data->active = false;
                    cancel_data->curl_code = CURLE_ABORTED_BY_CALLBACK;
                    wake_up_waker(&cancel_data->waker);
                }
                wake_up_waker(&message->cancel_payload.waker);
                break;
            }
            case RESUME_REQUEST: {
                assert(message->resume_payload.easy);
                hs_easy_data_t *resume_data = NULL;
                curl_easy_getinfo(message->resume_payload.easy, CURLINFO_PRIVATE, &resume_data);
                if (resume_data->active) {
                    curl_easy_pause(message->resume_payload.easy, CURLPAUSE_CONT);
                    check_multi_info(context->multi);
                }
                break;
            }
            case STOP_AGENT: {
                uv_stop(context->loop);
                break;
            }
        }
        destroy_outer_message(message);
    }
}

uv_async_t *init_async_check_messages(uv_loop_t *loop, mpsc_t *queue, CURLM *multi) {
    uv_async_t *uv_async = malloc(sizeof(uv_async_t));
    async_messages_context_t *async_messages_context = malloc(sizeof(async_messages_context_t));
    async_messages_context->loop = loop;
    async_messages_context->multi = multi;
    async_messages_context->chan = queue;
    uv_async->data = async_messages_context;
    uv_async_init(loop, uv_async, async_check_outer_messages);
    return uv_async;
}
