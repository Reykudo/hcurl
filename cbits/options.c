#include "options.h"

CURLcode hcurl_raw_easy_setopt_long(CURL *easy, int option, long value) {
    if (!easy) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    return curl_easy_setopt(easy, (CURLoption)option, value);
}

CURLcode hcurl_raw_easy_setopt_string(CURL *easy, int option,
                                      const char *value) {
    if (!easy || !value) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    return curl_easy_setopt(easy, (CURLoption)option, value);
}

CURLcode hcurl_easy_setopt_long(CURL *easy, enum hcurl_easy_option option,
                                long value) {
    if (!easy) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    switch (option) {
        case HCURL_OPT_HTTP_VERSION:
            return curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, value);
        case HCURL_OPT_PIPE_WAIT:
#if LIBCURL_VERSION_NUM >= 0x072b00
            return curl_easy_setopt(easy, CURLOPT_PIPEWAIT, value);
#else
            return CURLE_BAD_FUNCTION_ARGUMENT;
#endif
        case HCURL_OPT_FOLLOW_LOCATION:
            return curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, value);
        case HCURL_OPT_NO_SIGNAL:
            return curl_easy_setopt(easy, CURLOPT_NOSIGNAL, value);
        case HCURL_OPT_SSL_VERIFY_PEER:
            return curl_easy_setopt(easy, CURLOPT_SSL_VERIFYPEER, value);
        case HCURL_OPT_TCP_FAST_OPEN:
#if LIBCURL_VERSION_NUM >= 0x073100
            return curl_easy_setopt(easy, CURLOPT_TCP_FASTOPEN, value);
#else
            return CURLE_BAD_FUNCTION_ARGUMENT;
#endif
        case HCURL_OPT_TCP_KEEP_ALIVE:
#if LIBCURL_VERSION_NUM >= 0x071900
            return curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, value);
#else
            return CURLE_BAD_FUNCTION_ARGUMENT;
#endif
        case HCURL_OPT_TIMEOUT_MS:
            return curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, value);
        case HCURL_OPT_CONNECT_TIMEOUT_MS:
            return curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS, value);
        case HCURL_OPT_LOW_SPEED_TIME:
            return curl_easy_setopt(easy, CURLOPT_LOW_SPEED_TIME, value);
        case HCURL_OPT_LOW_SPEED_LIMIT:
            return curl_easy_setopt(easy, CURLOPT_LOW_SPEED_LIMIT, value);
        case HCURL_OPT_IP_RESOLVE:
            return curl_easy_setopt(easy, CURLOPT_IPRESOLVE, value);
        case HCURL_OPT_SSL_VERIFY_HOST:
            return curl_easy_setopt(easy, CURLOPT_SSL_VERIFYHOST, value);
        case HCURL_OPT_ACCEPT_ENCODING:
        default:
            return CURLE_BAD_FUNCTION_ARGUMENT;
    }
}

CURLcode hcurl_easy_setopt_string(CURL *easy, enum hcurl_easy_option option,
                                  const char *value) {
    if (!easy || !value) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    switch (option) {
        case HCURL_OPT_ACCEPT_ENCODING:
#if LIBCURL_VERSION_NUM >= 0x071506
            return curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, value);
#else
            return curl_easy_setopt(easy, CURLOPT_ENCODING, value);
#endif
        default:
            return CURLE_BAD_FUNCTION_ARGUMENT;
    }
}
