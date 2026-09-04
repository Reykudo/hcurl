#pragma once

#include "extras.h"

#include <HsFFI.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct header_data_s header_data_t;

enum header_wait_result {
    HEADER_WAIT_REGISTERED = 0,
    HEADER_WAIT_READY = 1,
    HEADER_WAIT_INVALID = 2
};

header_data_t *header_data_create(size_t initial_capacity);

CURLcode header_data_install(CURL *easy, header_data_t *headers);

void free_header_data(header_data_t *headers);

size_t header_callback(char *buffer, size_t size, size_t nitems, void *userdata);

/* Return a malloc-owned snapshot of the most recently completed HTTP header
 * block.  Redirect and informational blocks are replaced, never mixed. */
bool header_data_snapshot(header_data_t *headers, char **buffer, size_t *length,
                          long *status_code, int *terminal_before_response,
                          int *terminal_code);

enum header_wait_result header_data_wait(header_data_t *headers,
                                         HsStablePtr mvar, int capability);

/* Returns true only when ownership of the not-yet-fired StablePtr is handed
 * back to the caller. */
bool header_data_cancel_wait(header_data_t *headers, HsStablePtr mvar);

void header_data_mark_body_started(header_data_t *headers);

void header_data_mark_complete(header_data_t *headers, CURLcode terminal_code);
