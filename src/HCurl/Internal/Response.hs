{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Response (
    getBufferedResponse,
    getCompletedHttpParts,
    getHttpParts,
    getHttpPartsState,
    getTransferResult,
)
where

import Control.Concurrent.MVar (readMVar)
import Control.Exception (mask_, onException, throwIO)
import Control.Monad (when)
import Data.ByteString.Lazy qualified as BSL
import Data.ByteString.Unsafe qualified as BSU
import Foreign
import Foreign.C.Types
import HCurl.Internal.Easy (RequestHandler (..))
import HCurl.Internal.Headers (extractHeaderBlock, extractHeaderBlockState, parseHeaders)
import HCurl.Internal.Metrics (Metrics, metricsCount, peekMetrics)
import HCurl.Internal.Raw.Context (localCtx)
import HCurl.Internal.Raw.Curl (CurlCode (Ok))
import HCurl.Internal.Raw.Extras (EasyData, withEasyData)
import HCurl.Internal.Raw.Headers (HeadersData, withHeadersData)
import HCurl.Internal.Raw.Metrics (CurlMetricsContext, withCurlMetricsContext)
import HCurl.Internal.Raw.SimpleString (SimpleStringPtr, withSimpleStringPtr)
import HCurl.Internal.Result (getResponseCode)
import HCurl.Response (HttpParts (..), Response (..))
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> localCtx)

C.include "<stdint.h>"
C.include "<stdlib.h>"
C.include "curl_metrics.h"
C.include "extras.h"
C.include "headers.h"
C.include "simple_string.h"

getBufferedResponse :: RequestHandler response -> SimpleStringPtr -> IO (Either CurlCode (Response BSL.ByteString))
getBufferedResponse requestHandler responseTarget = do
    readMVar requestHandler.doneRequest
    takeBufferedResponse
        requestHandler.easyData
        requestHandler.metricsContext
        requestHandler.requestHeaders
        responseTarget

-- Completion data is copied out by one C call. Apart from reducing FFI
-- crossings, doing the snapshot together prevents a mixture of pre- and
-- post-cleanup state from the separately owned C objects.
takeBufferedResponse :: EasyData -> CurlMetricsContext -> HeadersData -> SimpleStringPtr -> IO (Either CurlCode (Response BSL.ByteString))
takeBufferedResponse easyData metricsContext headerData responseTarget = mask_ $
    withEasyData easyData \easyDataPtr ->
        withCurlMetricsContext metricsContext \metricsPtr ->
            withHeadersData headerData \headersPtr ->
                withSimpleStringPtr responseTarget \bodyTargetPtr ->
                    alloca \curlCodePtr ->
                        alloca \bodyPtrPtr ->
                            alloca \bodyLengthPtr ->
                                alloca \headerPtrPtr ->
                                    alloca \headerLengthPtr ->
                                        alloca \statusCodePtr ->
                                            allocaArray metricsCount \metricValuesPtr -> do
                                                let rawMetricsPtr = castPtr metricValuesPtr
                                                snapshotStatus <-
                                                    [CU.block| int {
                                                        int code = (int)hs_easy_data_code(
                                                            $(hs_easy_data_t* easyDataPtr));
                                                        *$(int* curlCodePtr) = code;
                                                        *$(char** bodyPtrPtr) = NULL;
                                                        *$(size_t* bodyLengthPtr) = 0;
                                                        *$(char** headerPtrPtr) = NULL;
                                                        *$(size_t* headerLengthPtr) = 0;
                                                        *$(long* statusCodePtr) = 0;

                                                        if (code != (int)CURLE_OK) {
                                                            return 0;
                                                        }
                                                        if (!header_data_snapshot(
                                                                $(header_data_t* headersPtr),
                                                                $(char** headerPtrPtr),
                                                                $(size_t* headerLengthPtr),
                                                                $(long* statusCodePtr),
                                                                NULL, NULL)) {
                                                            return 1;
                                                        }
                                                        char *body = simple_string_take(
                                                            $(simple_string_t* bodyTargetPtr),
                                                            $(size_t* bodyLengthPtr));
                                                        if (body == NULL) {
                                                            free(*$(char** headerPtrPtr));
                                                            *$(char** headerPtrPtr) = NULL;
                                                            return 2;
                                                        }
                                                        *$(char** bodyPtrPtr) = body;
                                                        curl_metrics_snapshot(
                                                            $(curl_metrics_context_t* metricsPtr),
                                                            (int64_t*)$(void* rawMetricsPtr));
                                                        if (*$(long* statusCodePtr) == 0) {
                                                            *$(long* statusCodePtr) =
                                                                hs_easy_data_response_code(
                                                                    $(hs_easy_data_t* easyDataPtr));
                                                        }
                                                        return 0;
                                                    } |]
                                                curlCode <- decodeCurlCode <$> peek curlCodePtr
                                                if curlCode /= Ok
                                                    then pure $ Left curlCode
                                                    else case snapshotStatus of
                                                        0 -> do
                                                            bodyPtr <- peek bodyPtrPtr
                                                            bodyLength <- peek bodyLengthPtr
                                                            headerPtr <- peek headerPtrPtr
                                                            headerLength <- peek headerLengthPtr
                                                            statusCode <- fromIntegral <$> (peek statusCodePtr :: IO CLong)
                                                            checkSnapshotLengths bodyPtr bodyLength headerPtr headerLength
                                                            bodyBytes <-
                                                                BSU.unsafePackMallocCStringLen
                                                                    (bodyPtr, fromIntegral bodyLength)
                                                                    `onException` (free bodyPtr >> free headerPtr)
                                                            headerBytes <-
                                                                BSU.unsafePackMallocCStringLen
                                                                    (headerPtr, fromIntegral headerLength)
                                                                    `onException` free headerPtr
                                                            metrics <- peekMetrics metricValuesPtr
                                                            let info = HttpParts{statusCode, headers = parseHeaders headerBytes}
                                                            pure . Right $! Response{info, body = BSL.fromStrict bodyBytes, metrics}
                                                        1 -> throwIO $ userError "hcurl: unable to snapshot response headers"
                                                        _ -> throwIO $ userError "hcurl: response body was already consumed"

checkSnapshotLengths :: Ptr CChar -> CSize -> Ptr CChar -> CSize -> IO ()
checkSnapshotLengths bodyPtr bodyLength headerPtr headerLength = do
    when (toInteger bodyLength > toInteger (maxBound :: Int)) $
        free bodyPtr >> free headerPtr >> throwIO (userError "hcurl: response body exceeds Haskell Int")
    when (toInteger headerLength > toInteger (maxBound :: Int)) $
        free bodyPtr >> free headerPtr >> throwIO (userError "hcurl: response headers exceed Haskell Int")

-- | Read the final curl result and metrics with one Haskell-to-C transition.
getTransferResult :: EasyData -> CurlMetricsContext -> IO (Either CurlCode Metrics)
getTransferResult easyData metricsContext =
    withEasyData easyData \easyDataPtr ->
        withCurlMetricsContext metricsContext \metricsPtr ->
            alloca \curlCodePtr ->
                allocaArray metricsCount \metricValuesPtr -> do
                    let rawMetricsPtr = castPtr metricValuesPtr
                    [CU.block| void {
                        int code = (int)hs_easy_data_code($(hs_easy_data_t* easyDataPtr));
                        *$(int* curlCodePtr) = code;
                        if (code == (int)CURLE_OK) {
                            curl_metrics_snapshot(
                                $(curl_metrics_context_t* metricsPtr),
                                (int64_t*)$(void* rawMetricsPtr));
                        }
                    } |]
                    curlCode <- decodeCurlCode <$> peek curlCodePtr
                    if curlCode == Ok
                        then Right <$> peekMetrics metricValuesPtr
                        else pure $ Left curlCode

decodeCurlCode :: CInt -> CurlCode
decodeCurlCode = toEnum . fromIntegral

-- | Snapshot the latest complete HTTP header block without a getinfo call.
getHttpParts :: HeadersData -> IO HttpParts
getHttpParts headers = do
    (statusCode, responseHeaders) <- extractHeaderBlock headers
    pure HttpParts{statusCode, headers = responseHeaders}

{- | Snapshot response headers together with a terminal result, when the
transfer completed before a final-looking response block was published.
-}
getHttpPartsState :: HeadersData -> IO (HttpParts, Maybe CurlCode)
getHttpPartsState headers = do
    (statusCode, responseHeaders, terminalResult) <- extractHeaderBlockState headers
    pure (HttpParts{statusCode, headers = responseHeaders}, terminalResult)

{- | Read the final response block after completion. The result cell is only
a fallback for protocols where libcurl did not deliver a textual status
line to the header callback.
-}
getCompletedHttpParts :: EasyData -> HeadersData -> IO HttpParts
getCompletedHttpParts easyData headerData = do
    (headerStatus, responseHeaders) <- extractHeaderBlock headerData
    statusCode <- if headerStatus == 0 then getResponseCode easyData else pure headerStatus
    pure HttpParts{statusCode, headers = responseHeaders}
