#pragma once

#include <curl/curl.h>

enum hcurl_easy_option {
    HCURL_OPT_HTTP_VERSION = 0,
    HCURL_OPT_PIPE_WAIT,
    HCURL_OPT_FOLLOW_LOCATION,
    HCURL_OPT_NO_SIGNAL,
    HCURL_OPT_SSL_VERIFY_PEER,
    HCURL_OPT_TCP_FAST_OPEN,
    HCURL_OPT_TCP_KEEP_ALIVE,
    HCURL_OPT_TIMEOUT_MS,
    HCURL_OPT_CONNECT_TIMEOUT_MS,
    HCURL_OPT_LOW_SPEED_TIME,
    HCURL_OPT_LOW_SPEED_LIMIT,
    HCURL_OPT_ACCEPT_ENCODING,
    HCURL_OPT_IP_RESOLVE,
    HCURL_OPT_SSL_VERIFY_HOST
};

/* Type-safe fixed-arity adapters for the variadic curl_easy_setopt API. */
CURLcode hcurl_raw_easy_setopt_long(CURL *easy, int option, long value);

CURLcode hcurl_raw_easy_setopt_string(CURL *easy, int option,
                                      const char *value);

CURLcode hcurl_easy_setopt_long(CURL *easy, enum hcurl_easy_option option,
                                long value);

CURLcode hcurl_easy_setopt_string(CURL *easy, enum hcurl_easy_option option,
                                  const char *value);
