#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "simple_string.h"

struct simple_string_s {
    char *ptr;
    size_t len;
    size_t capacity;
    size_t max_len;
};

simple_string_t *simple_string_create(size_t max_len) {
    simple_string_t *str = calloc(1, sizeof(*str));
    if (!str) {
        return NULL;
    }

    str->len = 0;
    str->max_len = max_len;
    str->capacity = 4096;
    if (max_len != SIZE_MAX && str->capacity > max_len + 1) {
        str->capacity = max_len + 1;
    }
    if (str->capacity == 0) {
        str->capacity = 1;
    }
    str->ptr = malloc(str->capacity);
    if (!str->ptr) {
        free(str);
        return NULL;
    }
    str->ptr[0] = '\0';
    return str;
}

CURLcode simple_string_install(CURL *easy, simple_string_t *str) {
    if (!easy || !str) {
        return CURLE_BAD_FUNCTION_ARGUMENT;
    }
    CURLcode code = curl_easy_setopt(easy, CURLOPT_WRITEDATA, str);
    if (code != CURLE_OK) return code;
    return curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, simple_string_writefunc);
}

void simple_string_destroy(simple_string_t *str) {
    if (!str) {
        return;
    }
    free(str->ptr);
    free(str);
}

char *simple_string_take(simple_string_t *str, size_t *len) {
    if (!str || !len) {
        return NULL;
    }
    char *ptr = str->ptr;
    *len = str->len;
    str->ptr = NULL;
    str->len = 0;
    str->capacity = 0;
    return ptr;
}

size_t simple_string_writefunc(char *ptr, size_t size, size_t nmemb, void *userdata) {
    simple_string_t *str = userdata;
    if (!str || !str->ptr || (size != 0 && nmemb > SIZE_MAX / size)) {
        return 0;
    }
    size_t appended = size * nmemb;
    if (appended == 0) {
        return 0;
    }
    if (!ptr || appended > SIZE_MAX - str->len) {
        return 0;
    }
    size_t new_len = str->len + appended;
    if (new_len > str->max_len) {
        return 0;
    }
    if (new_len == (size_t) -1) {
        return 0;
    }
    if (new_len + 1 > str->capacity) {
        size_t capacity = str->capacity ? str->capacity : 4096;
        while (capacity < new_len + 1) {
            if (capacity > (size_t) -1 / 2) {
                capacity = new_len + 1;
                break;
            }
            capacity *= 2;
        }
        char *resized = realloc(str->ptr, capacity);
        if (!resized) {
            return 0;
        }
        str->ptr = resized;
        str->capacity = capacity;
    }
    memcpy(str->ptr + str->len, ptr, appended);
    str->ptr[new_len] = '\0';
    str->len = new_len;
    return appended;
}
