#include "headers.h"

#include <curl/curl.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

struct header_data_s {
    uv_mutex_t mutex;
    char *current;
    size_t current_size;
    size_t current_capacity;
    char *completed;
    size_t completed_size;
    size_t completed_capacity;
    long current_status;
    long completed_status;
    bool in_block;
    bool ready;
    bool response_available;
    bool transfer_completed;
    CURLcode terminal_code;
    bool waiter_present;
    hs_waker_t waiter;
};

static bool reserve(char **buffer, size_t *capacity, size_t required) {
    if (required <= *capacity) {
        return true;
    }
    size_t grown = *capacity > 0 ? *capacity : 1;
    while (grown < required) {
        if (grown > SIZE_MAX / 2) {
            grown = required;
            break;
        }
        grown *= 2;
    }
    char *resized = realloc(*buffer, grown);
    if (!resized) {
        return false;
    }
    *buffer = resized;
    *capacity = grown;
    return true;
}

static bool append(char **buffer, size_t *length, size_t *capacity,
                   const char *data, size_t data_length) {
    if (data_length > SIZE_MAX - *length - 1) {
        return false;
    }
    size_t required = *length + data_length + 1;
    if (!reserve(buffer, capacity, required)) {
        return false;
    }
    memcpy(*buffer + *length, data, data_length);
    *length += data_length;
    (*buffer)[*length] = '\0';
    return true;
}

static bool is_status_line(const char *line, size_t length) {
    return length >= 5 && memcmp(line, "HTTP/", 5) == 0;
}

static long parse_status(const char *line, size_t length) {
    const char *end = line + length;
    const char *space = memchr(line, ' ', length);
    if (!space) {
        return 0;
    }
    while (space < end && *space == ' ') {
        ++space;
    }
    long status = 0;
    int digits = 0;
    while (space < end && *space >= '0' && *space <= '9' && digits < 3) {
        status = status * 10 + (*space - '0');
        ++space;
        ++digits;
    }
    return digits == 3 ? status : 0;
}

static bool is_blank_line(const char *line, size_t length) {
    return (length == 1 && line[0] == '\n')
        || (length == 2 && line[0] == '\r' && line[1] == '\n');
}

static unsigned char ascii_lower(unsigned char value) {
    return value >= 'A' && value <= 'Z' ? (unsigned char)(value + ('a' - 'A')) : value;
}

static bool contains_ascii_case_insensitive(const char *data, size_t length,
                                            const char *needle) {
    size_t needle_length = strlen(needle);
    if (!data || needle_length == 0 || length < needle_length) {
        return false;
    }
    for (size_t offset = 0; offset <= length - needle_length; ++offset) {
        size_t index = 0;
        while (index < needle_length
               && ascii_lower((unsigned char)data[offset + index])
                   == ascii_lower((unsigned char)needle[index])) {
            ++index;
        }
        if (index == needle_length) {
            return true;
        }
    }
    return false;
}

header_data_t *header_data_create(size_t initial_capacity) {
    size_t capacity = initial_capacity > 0 ? initial_capacity : 1;
    header_data_t *headers = calloc(1, sizeof(*headers));
    if (!headers) {
        return NULL;
    }
    headers->current = malloc(capacity);
    headers->completed = malloc(capacity);
    if (!headers->current || !headers->completed
        || uv_mutex_init(&headers->mutex) != 0) {
        free(headers->current);
        free(headers->completed);
        free(headers);
        return NULL;
    }
    headers->current[0] = '\0';
    headers->completed[0] = '\0';
    headers->current_capacity = capacity;
    headers->completed_capacity = capacity;
    return headers;
}

CURLcode header_data_install(CURL *easy, header_data_t *headers) {
    if (!easy || !headers) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    CURLcode code = curl_easy_setopt(easy, CURLOPT_HEADERDATA, headers);
    if (code != CURLE_OK) return code;
    code = curl_easy_setopt(easy, CURLOPT_HEADERFUNCTION, header_callback);
    if (code != CURLE_OK) return code;
#if LIBCURL_VERSION_NUM >= 0x073600
    code = curl_easy_setopt(easy, CURLOPT_SUPPRESS_CONNECT_HEADERS, 1L);
#endif
    return code;
}

void free_header_data(header_data_t *headers) {
    if (!headers) {
        return;
    }
    hs_waker_t waiter = {0};
    bool wake_waiter = false;
    uv_mutex_lock(&headers->mutex);
    if (headers->waiter_present) {
        waiter = headers->waiter;
        headers->waiter_present = false;
        wake_waiter = true;
    }
    uv_mutex_unlock(&headers->mutex);
    if (wake_waiter) {
        wake_up_waker(&waiter);
    }
    uv_mutex_destroy(&headers->mutex);
    free(headers->current);
    free(headers->completed);
    free(headers);
}

size_t header_callback(char *buffer, size_t size, size_t nitems, void *userdata) {
    header_data_t *headers = userdata;
    if (!headers || (size != 0 && nitems > SIZE_MAX / size)) {
        return 0;
    }
    size_t total = size * nitems;
    if (total == 0) {
        return 0;
    }

    bool success = true;
    bool wake_waiter = false;
    hs_waker_t waiter = {0};
    uv_mutex_lock(&headers->mutex);
    if (is_status_line(buffer, total)) {
        headers->current_size = 0;
        headers->current[0] = '\0';
        headers->current_status = parse_status(buffer, total);
        headers->in_block = true;
    }

    if (headers->in_block) {
        success = append(&headers->current, &headers->current_size,
                         &headers->current_capacity, buffer, total);
        if (success && is_blank_line(buffer, total)) {
            char *old_completed = headers->completed;
            size_t old_capacity = headers->completed_capacity;
            headers->completed = headers->current;
            headers->completed_size = headers->current_size;
            headers->completed_capacity = headers->current_capacity;
            headers->completed_status = headers->current_status;
            headers->current = old_completed;
            headers->current_size = 0;
            headers->current_capacity = old_capacity;
            headers->current[0] = '\0';
            headers->in_block = false;
            long status = headers->completed_status;
            bool proxy_connect = status == 200
                && contains_ascii_case_insensitive(
                    headers->completed, headers->completed_size,
                    "connection established");
            /* 101 switches protocols and is final; other 1xx blocks are
             * informational and must be replaced by the following block. */
            bool publish_early = (status == 101 || status >= 200)
                && !(status >= 300 && status < 400)
                && status != 401 && status != 407 && !proxy_connect;
            if (publish_early) {
                headers->response_available = true;
                headers->ready = true;
                if (headers->waiter_present) {
                    waiter = headers->waiter;
                    headers->waiter_present = false;
                    wake_waiter = true;
                }
            }
        }
    } else if (headers->completed_size > 0) {
        /* Header lines received after the response block are trailers. */
        success = append(&headers->completed, &headers->completed_size,
                         &headers->completed_capacity, buffer, total);
    }
    uv_mutex_unlock(&headers->mutex);
    if (wake_waiter) {
        wake_up_waker(&waiter);
    }
    return success ? total : 0;
}

bool header_data_snapshot(header_data_t *headers, char **buffer, size_t *length,
                          long *status_code, int *terminal_before_response,
                          int *terminal_code) {
    if (!headers || !buffer || !length || !status_code) {
        return false;
    }
    uv_mutex_lock(&headers->mutex);
    size_t snapshot_length = headers->completed_size;
    char *snapshot = malloc(snapshot_length + 1);
    if (snapshot) {
        memcpy(snapshot, headers->completed, snapshot_length);
        snapshot[snapshot_length] = '\0';
        *buffer = snapshot;
        *length = snapshot_length;
        *status_code = headers->completed_status;
        if (terminal_before_response) {
            *terminal_before_response =
                headers->transfer_completed && !headers->response_available ? 1 : 0;
        }
        if (terminal_code) {
            *terminal_code = (int)headers->terminal_code;
        }
    }
    uv_mutex_unlock(&headers->mutex);
    return snapshot != NULL;
}

enum header_wait_result header_data_wait(header_data_t *headers,
                                         HsStablePtr mvar, int capability) {
    if (!headers || !mvar) {
        return HEADER_WAIT_INVALID;
    }
    uv_mutex_lock(&headers->mutex);
    if (headers->ready) {
        uv_mutex_unlock(&headers->mutex);
        return HEADER_WAIT_READY;
    }
    if (headers->waiter_present) {
        uv_mutex_unlock(&headers->mutex);
        return HEADER_WAIT_INVALID;
    }
    headers->waiter.mvar = mvar;
    headers->waiter.capability = capability;
    headers->waiter.waked = false;
    headers->waiter_present = true;
    uv_mutex_unlock(&headers->mutex);
    return HEADER_WAIT_REGISTERED;
}

bool header_data_cancel_wait(header_data_t *headers, HsStablePtr mvar) {
    if (!headers || !mvar) {
        return false;
    }
    bool removed = false;
    uv_mutex_lock(&headers->mutex);
    if (headers->waiter_present && headers->waiter.mvar == mvar) {
        headers->waiter_present = false;
        headers->waiter.mvar = NULL;
        removed = true;
    }
    uv_mutex_unlock(&headers->mutex);
    return removed;
}

static void mark_ready(header_data_t *headers, bool response_available) {
    if (!headers) {
        return;
    }
    hs_waker_t waiter = {0};
    bool wake_waiter = false;
    uv_mutex_lock(&headers->mutex);
    if (response_available && headers->completed_status != 0) {
        headers->response_available = true;
    }
    headers->ready = true;
    if (headers->waiter_present) {
        waiter = headers->waiter;
        headers->waiter_present = false;
        wake_waiter = true;
    }
    uv_mutex_unlock(&headers->mutex);
    if (wake_waiter) {
        wake_up_waker(&waiter);
    }
}

void header_data_mark_body_started(header_data_t *headers) {
    mark_ready(headers, true);
}

void header_data_mark_complete(header_data_t *headers, CURLcode terminal_code) {
    if (!headers) {
        return;
    }
    hs_waker_t waiter = {0};
    bool wake_waiter = false;
    uv_mutex_lock(&headers->mutex);
    headers->transfer_completed = true;
    headers->terminal_code = terminal_code;
    headers->ready = true;
    if (headers->waiter_present) {
        waiter = headers->waiter;
        headers->waiter_present = false;
        wake_waiter = true;
    }
    uv_mutex_unlock(&headers->mutex);
    if (wake_waiter) {
        wake_up_waker(&waiter);
    }
}
