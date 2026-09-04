module HCurl.Internal.Raw.Metrics where

#include "curl_metrics.h"

{# pointer *curl_metrics_context_t as CurlMetricsContext foreign newtype #}

rawMetricsCount :: Int
rawMetricsCount = {#const HCURL_METRICS_COUNT#}
