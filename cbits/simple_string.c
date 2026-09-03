#include <stdlib.h>
#include <string.h>
#include "simple_string.h"

// TODO: check malloc result
void init_simple_string(simple_string_t *str) {
    str->len = 0;
    str->ptr = malloc(str->len + 1);
    str->ptr[0] = '\0';
}

size_t simple_string_writefunc(void *ptr, size_t size, size_t nmemb, simple_string_t *str) {
    if (size && nmemb > ((size_t) -1 - str->len) / size) {
        /* overflow: cannot represent this body */
        return 0;
    }
    size_t new_len = str->len + size * nmemb;
    if (new_len == (size_t) -1) {
        return 0;
    }
    str->ptr = realloc(str->ptr, new_len + 1);

    // fail request when allocation fails
    if (str->ptr == NULL) {
        return 0;
    }

    memcpy(str->ptr + str->len, ptr, size * nmemb);
    str->ptr[new_len] = '\0';
    str->len = new_len;

    return size * nmemb;
}
