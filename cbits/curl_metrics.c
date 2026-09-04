#define CURL_DISABLE_DEPRECATION
#include "curl_metrics.h"

#include <limits.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>

typedef struct curl_metrics_s {
    _Atomic int64_t upload_progress;
    _Atomic int64_t upload_total;
    _Atomic int64_t download_progress;
    _Atomic int64_t download_total;
    _Atomic int64_t upload_speed;
    _Atomic int64_t download_speed;
    _Atomic int64_t namelookup_time;
    _Atomic int64_t connect_time;
    _Atomic int64_t appconnect_time;
    _Atomic int64_t pretransfer_time;
    _Atomic int64_t starttransfer_time;
    _Atomic int64_t total_time;
    _Atomic int64_t redirect_time;
} curl_metrics_t;

struct curl_metrics_context_s {
    curl_metrics_t metrics;
    bool streamed_download;
    bool streamed_upload;
};

curl_metrics_context_t *curl_metrics_context_create(void) {
    curl_metrics_context_t *context = malloc(sizeof(*context));
    if (!context) {
        return NULL;
    }
#define INITIALIZE_METRIC(field) atomic_init(&context->metrics.field, 0)
    INITIALIZE_METRIC(upload_progress);
    INITIALIZE_METRIC(upload_total);
    INITIALIZE_METRIC(download_progress);
    INITIALIZE_METRIC(download_total);
    INITIALIZE_METRIC(upload_speed);
    INITIALIZE_METRIC(download_speed);
    INITIALIZE_METRIC(namelookup_time);
    INITIALIZE_METRIC(connect_time);
    INITIALIZE_METRIC(appconnect_time);
    INITIALIZE_METRIC(pretransfer_time);
    INITIALIZE_METRIC(starttransfer_time);
    INITIALIZE_METRIC(total_time);
    INITIALIZE_METRIC(redirect_time);
#undef INITIALIZE_METRIC
    context->streamed_download = false;
    context->streamed_upload = false;
    return context;
}

void curl_metrics_context_destroy(curl_metrics_context_t *context) {
    free(context);
}

void curl_metrics_streamed_download(curl_metrics_context_t *context) {
    if (context) {
        context->streamed_download = true;
    }
}

void curl_metrics_streamed_upload(curl_metrics_context_t *context) {
    if (context) {
        context->streamed_upload = true;
    }
}

static void add_bytes(_Atomic int64_t *counter, size_t bytes) {
    int64_t current = atomic_load_explicit(counter, memory_order_relaxed);
    for (;;) {
        int64_t increment = bytes > (size_t)INT64_MAX
            ? INT64_MAX
            : (int64_t)bytes;
        int64_t next = current > INT64_MAX - increment
            ? INT64_MAX
            : current + increment;
        if (atomic_compare_exchange_weak_explicit(
                counter, &current, next,
                memory_order_relaxed, memory_order_relaxed)) {
            return;
        }
    }
}

void curl_metrics_add_downloaded(curl_metrics_context_t *context, size_t bytes) {
    if (context) {
        add_bytes(&context->metrics.download_progress, bytes);
    }
}

void curl_metrics_add_uploaded(curl_metrics_context_t *context, size_t bytes) {
    if (context) {
        add_bytes(&context->metrics.upload_progress, bytes);
    }
}

static int64_t nonnegative_double_to_int64(double value, double scale) {
    if (!(value > 0.0)) {
        return 0;
    }
    if (value >= (double)INT64_MAX / scale) {
        return INT64_MAX;
    }
    return (int64_t)(value * scale);
}

#if LIBCURL_VERSION_NUM >= 0x073700
static int64_t get_off_t_info(CURL *easy, CURLINFO info) {
    curl_off_t value = 0;
    if (curl_easy_getinfo(easy, info, &value) != CURLE_OK || value <= 0) {
        return 0;
    }
    return value >= (curl_off_t)INT64_MAX ? INT64_MAX : (int64_t)value;
}
#endif

static int64_t get_double_info(CURL *easy, CURLINFO info, double scale) {
    double value = 0.0;
    return curl_easy_getinfo(easy, info, &value) == CURLE_OK
        ? nonnegative_double_to_int64(value, scale)
        : 0;
}

void curl_metrics_finish(curl_metrics_context_t *context, CURL *easy) {
    if (!context || !easy) {
        return;
    }

#if LIBCURL_VERSION_NUM >= 0x071300
    const curl_version_info_data *runtime = curl_version_info(CURLVERSION_NOW);
    unsigned int runtime_version = runtime ? runtime->version_num : 0;
#endif

#if LIBCURL_VERSION_NUM >= 0x073700
#define STORE_SIZE_INFO(field, modern, legacy, scale) do { \
    int64_t metric_value = runtime_version >= 0x073700 \
        ? get_off_t_info(easy, modern) \
        : get_double_info(easy, legacy, scale); \
    atomic_store_explicit(&context->metrics.field, metric_value, \
                          memory_order_relaxed); \
} while (0)
#else
#define STORE_SIZE_INFO(field, modern, legacy, scale) \
    atomic_store_explicit(&context->metrics.field, get_double_info(easy, legacy, scale), \
                          memory_order_relaxed)
#endif
#if LIBCURL_VERSION_NUM >= 0x073d00
#define STORE_TIME_INFO(field, modern, legacy) do { \
    int64_t metric_value = runtime_version >= 0x073d00 \
        ? get_off_t_info(easy, modern) \
        : get_double_info(easy, legacy, 1000000.0); \
    atomic_store_explicit(&context->metrics.field, metric_value, \
                          memory_order_relaxed); \
} while (0)
#else
#define STORE_TIME_INFO(field, modern, legacy) \
    atomic_store_explicit(&context->metrics.field, get_double_info(easy, legacy, 1000000.0), \
                          memory_order_relaxed)
#endif
    if (!context->streamed_upload) {
        STORE_SIZE_INFO(upload_progress, CURLINFO_SIZE_UPLOAD_T, CURLINFO_SIZE_UPLOAD, 1.0);
    }
    STORE_SIZE_INFO(upload_total, CURLINFO_CONTENT_LENGTH_UPLOAD_T,
                    CURLINFO_CONTENT_LENGTH_UPLOAD, 1.0);
    if (!context->streamed_download) {
        STORE_SIZE_INFO(download_progress, CURLINFO_SIZE_DOWNLOAD_T,
                        CURLINFO_SIZE_DOWNLOAD, 1.0);
    }
    STORE_SIZE_INFO(download_total, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T,
                    CURLINFO_CONTENT_LENGTH_DOWNLOAD, 1.0);
    STORE_SIZE_INFO(upload_speed, CURLINFO_SPEED_UPLOAD_T, CURLINFO_SPEED_UPLOAD, 1.0);
    STORE_SIZE_INFO(download_speed, CURLINFO_SPEED_DOWNLOAD_T, CURLINFO_SPEED_DOWNLOAD, 1.0);
    STORE_TIME_INFO(namelookup_time, CURLINFO_NAMELOOKUP_TIME_T, CURLINFO_NAMELOOKUP_TIME);
    STORE_TIME_INFO(connect_time, CURLINFO_CONNECT_TIME_T, CURLINFO_CONNECT_TIME);
#if LIBCURL_VERSION_NUM >= 0x071300
    if (runtime_version >= 0x071300) {
        STORE_TIME_INFO(appconnect_time, CURLINFO_APPCONNECT_TIME_T,
                        CURLINFO_APPCONNECT_TIME);
    }
#endif
    STORE_TIME_INFO(pretransfer_time, CURLINFO_PRETRANSFER_TIME_T, CURLINFO_PRETRANSFER_TIME);
    STORE_TIME_INFO(starttransfer_time, CURLINFO_STARTTRANSFER_TIME_T, CURLINFO_STARTTRANSFER_TIME);
    STORE_TIME_INFO(total_time, CURLINFO_TOTAL_TIME_T, CURLINFO_TOTAL_TIME);
    STORE_TIME_INFO(redirect_time, CURLINFO_REDIRECT_TIME_T, CURLINFO_REDIRECT_TIME);
#undef STORE_SIZE_INFO
#undef STORE_TIME_INFO
}

void curl_metrics_snapshot(const curl_metrics_context_t *context,
                           int64_t output[HCURL_METRICS_COUNT]) {
    if (!output) {
        return;
    }
    if (!context) {
        for (size_t index = 0; index < HCURL_METRICS_COUNT; ++index) {
            output[index] = 0;
        }
        return;
    }

    output[0] = atomic_load_explicit(&context->metrics.upload_progress, memory_order_relaxed);
    output[1] = atomic_load_explicit(&context->metrics.upload_total, memory_order_relaxed);
    output[2] = atomic_load_explicit(&context->metrics.download_progress, memory_order_relaxed);
    output[3] = atomic_load_explicit(&context->metrics.download_total, memory_order_relaxed);
    output[4] = atomic_load_explicit(&context->metrics.upload_speed, memory_order_relaxed);
    output[5] = atomic_load_explicit(&context->metrics.download_speed, memory_order_relaxed);
    output[6] = atomic_load_explicit(&context->metrics.namelookup_time, memory_order_relaxed);
    output[7] = atomic_load_explicit(&context->metrics.connect_time, memory_order_relaxed);
    output[8] = atomic_load_explicit(&context->metrics.appconnect_time, memory_order_relaxed);
    output[9] = atomic_load_explicit(&context->metrics.pretransfer_time, memory_order_relaxed);
    output[10] = atomic_load_explicit(&context->metrics.starttransfer_time, memory_order_relaxed);
    output[11] = atomic_load_explicit(&context->metrics.total_time, memory_order_relaxed);
    output[12] = atomic_load_explicit(&context->metrics.redirect_time, memory_order_relaxed);
}
