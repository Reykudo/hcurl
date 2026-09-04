#pragma once

#include <HsFFI.h>
#include <curl/curl.h>
#include <stdbool.h>
#include <stdatomic.h>

typedef struct curl_slist curl_slist_t;

/* A one-shot RTS wake-up.  hs_try_putmvar consumes the StablePtr, therefore
 * a waker must never be fired twice or reused. */
typedef struct hs_waker_s {
    HsStablePtr mvar;
    int capability;
    bool waked;
} hs_waker_t;

typedef struct hs_easy_data_s {
    _Atomic int curl_code;
    _Atomic long response_code;
    hs_waker_t waker;
    _Atomic bool waker_fired;
    _Atomic bool completed;
} hs_easy_data_t;

hs_easy_data_t *hs_easy_data_create(HsStablePtr mvar, int capability);

void hs_easy_data_destroy(hs_easy_data_t *data);

void hs_easy_data_set_result(hs_easy_data_t *data, CURL *easy, CURLcode code);

void hs_easy_data_wake(hs_easy_data_t *data);

CURLcode hs_easy_data_code(const hs_easy_data_t *data);

long hs_easy_data_response_code(const hs_easy_data_t *data);

void wake_up_waker(hs_waker_t *waker);

/* Return a one-node overlay containing header unless the borrowed list already
 * contains that header name. The borrowed tail is never copied or mutated. */
curl_slist_t *hcurl_slist_overlay_header(curl_slist_t *borrowed,
                                         const char *header);

void hcurl_slist_free_overlay(curl_slist_t *head, curl_slist_t *borrowed);

CURLcode hcurl_global_init_once(void);
