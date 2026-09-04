#pragma once

#include <curl/curl.h>
#include <stddef.h>
#include <stdint.h>

typedef struct curl_metrics_context_s curl_metrics_context_t;

#define HCURL_METRICS_COUNT 13

curl_metrics_context_t *curl_metrics_context_create(void);

void curl_metrics_context_destroy(curl_metrics_context_t *context);

void curl_metrics_streamed_download(curl_metrics_context_t *context);

void curl_metrics_streamed_upload(curl_metrics_context_t *context);

void curl_metrics_add_downloaded(curl_metrics_context_t *context, size_t bytes);

void curl_metrics_add_uploaded(curl_metrics_context_t *context, size_t bytes);

/* Capture values that libcurl only exposes through curl_easy_getinfo.  This
 * is called once by the agent, immediately before curl_easy_cleanup. */
void curl_metrics_finish(curl_metrics_context_t *context, CURL *easy);

void curl_metrics_snapshot(const curl_metrics_context_t *context,
                           int64_t output[HCURL_METRICS_COUNT]);
