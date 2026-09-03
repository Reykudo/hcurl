#include <stdlib.h>
#include <string.h>
#include "simple_string.h"

void init_simple_string(simple_string_t *str) {
    str->len = 0;
    str->capacity = 4096;
    str->ptr = malloc(str->capacity);
    if (str->ptr) {
        str->ptr[0] = '\0';
    }
}

size_t simple_string_writefunc(void *ptr, size_t size, size_t nmemb, simple_string_t *str) {
    if (size && nmemb > ((size_t) -1 - str->len) / size) {
        return 0;
    }
    size_t new_len = str->len + size * nmemb;
    if (new_len == (size_t) -1) {
        return 0;
    }
    if (new_len + 1 > str->capacity) {
        size_t capacity = str->capacity ? str->capacity : 4096;
        while (capacity < new_len + 1) {
            if (capacity > (size_t) -1 / 2) {
                return 0;
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
    memcpy(str->ptr + str->len, ptr, size * nmemb);
    str->ptr[new_len] = '\0';
    str->len = new_len;
    return size * nmemb;
}
