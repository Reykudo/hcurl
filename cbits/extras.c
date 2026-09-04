#include "extras.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

static uv_once_t global_init_once = UV_ONCE_INIT;
static CURLcode global_init_result = CURLE_FAILED_INIT;

static void initialize_curl_global(void) {
    global_init_result = curl_global_init(CURL_GLOBAL_DEFAULT);
}

CURLcode hcurl_global_init_once(void) {
    uv_once(&global_init_once, initialize_curl_global);
    return global_init_result;
}

hs_easy_data_t *hs_easy_data_create(HsStablePtr mvar, int capability) {
    hs_easy_data_t *data = calloc(1, sizeof(*data));
    if (!data) {
        return NULL;
    }

    atomic_init(&data->curl_code, (int)CURLE_OK);
    atomic_init(&data->response_code, 0);
    data->waker.mvar = mvar;
    data->waker.capability = capability;
    data->waker.waked = false;
    atomic_init(&data->waker_fired, false);
    atomic_init(&data->completed, false);
    return data;
}

void hs_easy_data_destroy(hs_easy_data_t *data) {
    if (!data) {
        return;
    }

    /* hs_try_putmvar consumes this StablePtr asynchronously. Read the atomic
     * ownership flag rather than racing the agent's plain hs_waker_t state. */
    if (data->waker.mvar
        && !atomic_load_explicit(&data->waker_fired, memory_order_acquire)) {
        hs_free_stable_ptr(data->waker.mvar);
    }
    free(data);
}

void wake_up_waker(hs_waker_t *waker) {
    if (waker && waker->mvar && !waker->waked) {
        waker->waked = true;
        hs_try_putmvar(waker->capability, waker->mvar);
    }
}

static unsigned char ascii_lower(unsigned char value) {
    return value >= 'A' && value <= 'Z'
        ? (unsigned char)(value + ('a' - 'A'))
        : value;
}

static bool header_name_equal(const char *candidate, const char *header,
                              size_t name_length) {
    if (!candidate || !header) {
        return false;
    }
    for (size_t index = 0; index < name_length; ++index) {
        if (candidate[index] == '\0'
            || ascii_lower((unsigned char)candidate[index])
                != ascii_lower((unsigned char)header[index])) {
            return false;
        }
    }
    return true;
}

curl_slist_t *hcurl_slist_overlay_header(curl_slist_t *borrowed,
                                         const char *header) {
    if (!header) {
        return NULL;
    }
    const char *colon = strchr(header, ':');
    if (!colon) {
        return NULL;
    }
    size_t name_length = (size_t)(colon - header) + 1;
    for (curl_slist_t *cursor = borrowed; cursor; cursor = cursor->next) {
        if (header_name_equal(cursor->data, header, name_length)) {
            return borrowed;
        }
    }

    curl_slist_t *prefix = curl_slist_append(NULL, header);
    if (!prefix) {
        return NULL;
    }
    prefix->next = borrowed;
    return prefix;
}

void hcurl_slist_free_overlay(curl_slist_t *head, curl_slist_t *borrowed) {
    if (head && head != borrowed) {
        head->next = NULL;
        curl_slist_free_all(head);
    }
}

void hs_easy_data_set_result(hs_easy_data_t *data, CURL *easy, CURLcode code) {
    if (!data || atomic_load_explicit(&data->completed, memory_order_acquire)) {
        return;
    }

    atomic_store_explicit(&data->curl_code, (int)code, memory_order_relaxed);
    if (easy) {
        long response_code = 0;
        if (curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &response_code) == CURLE_OK) {
            atomic_store_explicit(&data->response_code, response_code,
                                  memory_order_relaxed);
        }
    }
    atomic_store_explicit(&data->completed, true, memory_order_release);
}

void hs_easy_data_wake(hs_easy_data_t *data) {
    if (!data || !atomic_load_explicit(&data->completed, memory_order_acquire)) {
        return;
    }
    bool expected = false;
    if (!atomic_compare_exchange_strong_explicit(
            &data->waker_fired, &expected, true,
            memory_order_acq_rel, memory_order_acquire)) {
        return;
    }
    wake_up_waker(&data->waker);
}

CURLcode hs_easy_data_code(const hs_easy_data_t *data) {
    if (!data) {
        return CURLE_FAILED_INIT;
    }
    /* set_result publishes the code, response status, and all preceding
     * completion work through this release/acquire pair.  Acquiring a
     * different atomic (curl_code itself is written relaxed) would not
     * establish that happens-before relationship on weakly ordered targets. */
    (void)atomic_load_explicit(&data->completed, memory_order_acquire);
    return (CURLcode)atomic_load_explicit(&data->curl_code,
                                          memory_order_relaxed);
}

long hs_easy_data_response_code(const hs_easy_data_t *data) {
    if (!data) {
        return 0;
    }
    (void)atomic_load_explicit(&data->completed, memory_order_acquire);
    return atomic_load_explicit(&data->response_code, memory_order_relaxed);
}
