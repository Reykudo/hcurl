{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Metrics (
    module HCurl.Metrics,
    extractMetrics,
    finalizerCurlMetricsContext,
    initCurlMetrics,
    metricsCount,
    peekMetrics,
) where

import Control.Exception (mask_, onException, throwIO)
import Foreign
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Metrics
import HCurl.Metrics
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> localCtx)

C.include "curl_metrics.h"

finalizerCurlMetricsContext :: FunPtr (Ptr CurlMetricsContext -> IO ())
finalizerCurlMetricsContext =
    [C.funPtr| void hcurl_metrics_finalizer(curl_metrics_context_t *context) {
        curl_metrics_context_destroy(context);
    } |]

initCurlMetrics :: IO CurlMetricsContext
initCurlMetrics = mask_ do
    ptr <- [CU.exp| curl_metrics_context_t* { curl_metrics_context_create() } |]
    if ptr == nullPtr
        then throwIO $ userError "hcurl: unable to allocate metrics context"
        else
            (CurlMetricsContext <$> newForeignPtr finalizerCurlMetricsContext ptr)
                `onException` [CU.block| void { curl_metrics_context_destroy($(curl_metrics_context_t* ptr)); } |]

extractMetrics :: CurlMetricsContext -> IO Metrics
extractMetrics context =
    withCurlMetricsContext context \contextPtr ->
        allocaArray metricsCount \valuesPtr -> do
            let rawValues = castPtr valuesPtr
            [CU.block| void {
                curl_metrics_snapshot($(curl_metrics_context_t* contextPtr),
                                      (int64_t*)$(void* rawValues));
            } |]
            peekMetrics valuesPtr

metricsCount :: Int
metricsCount = rawMetricsCount

peekMetrics :: Ptr Int64 -> IO Metrics
peekMetrics valuesPtr = do
    values <- peekArray metricsCount valuesPtr
    case values of
        [ uploadProgress
            , uploadTotal
            , downloadProgress
            , downloadTotal
            , uploadSpeed
            , downloadSpeed
            , namelookupTime
            , connectTime
            , appconnectTime
            , pretransferTime
            , starttransferTime
            , totalTime
            , redirectTime
            ] -> pure Metrics{..}
        _ -> throwIO $ userError "hcurl: invalid metrics snapshot"
