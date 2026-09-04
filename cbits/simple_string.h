#pragma once

#include <curl/curl.h>
#include <stddef.h>

typedef struct simple_string_s simple_string_t;

simple_string_t *simple_string_create(size_t max_len);

CURLcode simple_string_install(CURL *easy, simple_string_t *str);

void simple_string_destroy(simple_string_t *str);

char *simple_string_take(simple_string_t *str, size_t *len);

size_t simple_string_writefunc(char *ptr, size_t size, size_t nmemb, void *userdata);
