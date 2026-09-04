{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Easy (
    InvalidRequest (..),
    RequestBodyMode (..),
    RequestHandler (..),
    allocateEasy,
    checkCurlSetup,
    cleanupEasy,
    finalizerSimpleString,
    initRequest,
    initRequestWith,
    initRequestWithStreams,
    newEasy,
    setDefaultHTTPOptions,
    setEasyData,
    setHeaders,
    setMetrics,
    setRequestMethodAndBody,
    setRequestOpts,
    setSimpleStringResponse,
    setUserOptions,
    validateRequest,
) where

import Control.Monad (unless, when)
import Control.Monad.Trans.Resource
import Data.ByteString qualified as BS
import Data.Foldable
import Foreign (FunPtr, Ptr, newForeignPtr, nullPtr, touchForeignPtr)
import Foreign.C.Types
import HCurl.Internal.Headers (setHeaderReader)
import HCurl.Internal.Metrics
import HCurl.Internal.Options
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Extras (EasyData)
import HCurl.Internal.Raw.Headers (HeadersData)
import HCurl.Internal.Raw.MPSC (TransferStreams, noTransferStreams)
import HCurl.Internal.Raw.Metrics (CurlMetricsContext)
import HCurl.Internal.Raw.SimpleString
import HCurl.Internal.Result (mkEasyData)
import HCurl.Internal.Slist
import HCurl.Request
import HCurl.Types
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU
import UnliftIO

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<string.h>"
C.include "<stdlib.h>"
C.include "<stdint.h>"

C.include "<curl/curl.h>"
C.include "HsFFI.h"

C.include "simple_string.h"
C.include "extras.h"
C.include "curl_metrics.h"
C.include "headers.h"

newEasy :: IO CurlEasy
newEasy = do
    easyPtr <- [CU.exp|CURL* { curl_easy_init() }|]
    when (easyPtr == nullPtr) $ throwIO $ userError "hcurl: curl_easy_init failed"
    pure $ CurlEasy easyPtr

allocateEasy :: (MonadResource m) => m (ReleaseKey, CurlEasy)
allocateEasy =
    allocate
        newEasy
        cleanupEasy

cleanupEasy :: CurlEasy -> IO ()
cleanupEasy (CurlEasy easyPtr) =
    [CU.block|void { curl_easy_cleanup($(CURL* easyPtr)); }|]

setMetrics :: (MonadIO m) => m CurlMetricsContext
setMetrics = liftIO initCurlMetrics

setEasyData :: (MonadIO m) => m (MVar (), EasyData)
setEasyData = do
    doneRequest <- newEmptyMVar @_ @()
    easyData <- liftIO $ mkEasyData doneRequest
    pure (doneRequest, easyData)

finalizerSimpleString :: FunPtr (Ptr SimpleStringPtr -> IO ())
finalizerSimpleString =
    [C.funPtr| void hcurl_simple_string_finalizer(simple_string_t *string) {
        simple_string_destroy(string);
    } |]

setSimpleStringResponse :: (MonadIO m) => CurlEasy -> m SimpleStringPtr
setSimpleStringResponse (CurlEasy easyPtr) = liftIO . mask_ $ do
    simpleStringPtr <- [CU.exp| simple_string_t* { simple_string_create(SIZE_MAX) } |]
    when (simpleStringPtr == nullPtr) . throwIO $
        userError "hcurl: unable to allocate response buffer"
    simpleString <-
        (SimpleStringPtr <$> newForeignPtr finalizerSimpleString simpleStringPtr)
            `onException` [CU.block| void { simple_string_destroy($(simple_string_t* simpleStringPtr)); } |]
    liftIO $ withSimpleStringPtr simpleString \target ->
        checkCurlSetup
            [CU.exp| int {
                (int)simple_string_install(
                    $(CURL* easyPtr), $(simple_string_t* target))
            } |]
    pure simpleString

validateRequest :: RequestBodyMode -> Request -> IO ()
validateRequest bodyMode Request{..} = do
    when (BS.null url) $ throwIO RequestURLIsEmpty
    when (BS.elem 0 url) $ throwIO $ RequestContainsNul "URL"
    case BS.find isForbiddenURLControl url of
        Just byte -> throwIO $ RequestContainsControl "URL" byte
        Nothing -> pure ()
    when (timeoutMS < 0) $ throwIO $ NegativeRequestTimeout "timeoutMS" timeoutMS
    when (connectionTimeoutMS < 0) $
        throwIO $
            NegativeRequestTimeout "connectionTimeoutMS" connectionTimeoutMS
    when (lowSpeedLimit.timeout < 0) $
        throwIO $
            NegativeRequestTimeout "lowSpeedLimit.timeout" lowSpeedLimit.timeout
    when (lowSpeedLimit.lowSpeed < 0) $
        throwIO $
            NegativeRequestTimeout "lowSpeedLimit.lowSpeed" lowSpeedLimit.lowSpeed
    traverse_
        (uncurry validateCLong)
        [ ("timeoutMS", timeoutMS)
        , ("connectionTimeoutMS", connectionTimeoutMS)
        , ("lowSpeedLimit.timeout", lowSpeedLimit.timeout)
        , ("lowSpeedLimit.lowSpeed", lowSpeedLimit.lowSpeed)
        ]
    case method of
        Custom customMethod ->
            unless (validMethod customMethod) $ throwIO $ InvalidHTTPMethod customMethod
        _ -> pure ()
    case headers of
        HeaderList values -> for_ (zip [0 ..] values) \(index, value) ->
            if BS.elem 0 value
                then throwIO $ RequestHeaderContainsNul index
                else
                    when (BS.elem 10 value || BS.elem 13 value) $
                        throwIO $
                            RequestHeaderContainsNewline index
        _ -> pure ()
    case (bodyMode, method, body) of
        (UseRequestBody, Head, Buffer _) -> throwIO HeadRequestHasBody
        (UseStreamingRequestBody, Post, Empty) ->
            when (effectiveFollowLocation extraOptions == Just True) $
                throwIO StreamingUploadRedirectsUnsupported
        (UseStreamingRequestBody, Post, Buffer _) -> throwIO StreamingUploadRequiresEmptyBody
        (UseStreamingRequestBody, _, _) -> throwIO StreamingUploadRequiresPost
        _ -> pure ()
  where
    validateCLong name value = do
        let integerValue = toInteger value
        when
            ( integerValue < toInteger (minBound :: CLong)
                || integerValue > toInteger (maxBound :: CLong)
            )
            $ throwIO
            $ RequestValueOutOfRange name integerValue

    validMethod value =
        not (BS.null value)
            && BS.all (\byte -> byte >= 33 && byte <= 126 && not (BS.elem byte separators)) value
    separators = "()<>@,;:\\\"/[]?={}"
    isForbiddenURLControl byte = byte <= 32 || byte == 127
    effectiveFollowLocation =
        foldl'
            ( \current -> \case
                OptionFollowLocation enabled -> Just enabled
                _ -> current
            )
            Nothing

checkCurlSetup :: (MonadIO m) => IO CInt -> m ()
checkCurlSetup operation = liftIO do
    code <- operation
    unless (code == 0) $ throwIO (toEnum (fromIntegral code) :: CurlCode)

setRequestMethodAndBody :: (MonadIO m) => RequestBodyMode -> Request -> CurlEasy -> m ()
setRequestMethodAndBody bodyMode Request{method, body} (CurlEasy easyPtr) =
    case bodyMode of
        UseStreamingRequestBody ->
            checkCurlSetup
                [CU.block| int {
                    CURLcode code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_UPLOAD, 1L);
                    if (code != CURLE_OK) return (int)code;
                    code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_CUSTOMREQUEST, "POST");
                    if (code != CURLE_OK) return (int)code;
                    code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_INFILESIZE_LARGE,
                                            (curl_off_t)-1);
                    if (code != CURLE_OK) return (int)code;
                    code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_FOLLOWLOCATION, 0L);
                    return (int)code;
                } |]
        UseRequestBody -> case body of
            Empty -> setEmptyMethod method
            Buffer bytes -> setBufferedMethod method bytes
  where
    setEmptyMethod = \case
        Get ->
            checkCurlSetup [CU.exp| int { (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_HTTPGET, 1L) } |]
        Head ->
            checkCurlSetup [CU.exp| int { (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_NOBODY, 1L) } |]
        Post ->
            checkCurlSetup
                [CU.block| int {
                    CURLcode code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_POST, 1L);
                    if (code != CURLE_OK) return (int)code;
                    code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_POSTFIELDS, "");
                    if (code != CURLE_OK) return (int)code;
                    return (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_POSTFIELDSIZE_LARGE,
                                                 (curl_off_t)0);
                } |]
        customMethod -> setCustomMethod customMethod Nothing

    setBufferedMethod Head _ = throwIO HeadRequestHasBody
    setBufferedMethod Post bytes =
        checkCurlSetup
            [CU.block| int {
                CURLcode code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_POST, 1L);
                if (code != CURLE_OK) return (int)code;
                code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_POSTFIELDSIZE_LARGE,
                                        (curl_off_t)$bs-len:bytes);
                if (code != CURLE_OK) return (int)code;
                return (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_COPYPOSTFIELDS,
                                             $bs-ptr:bytes);
            } |]
    setBufferedMethod customMethod bytes = setCustomMethod customMethod (Just bytes)

    setCustomMethod customMethod maybeBytes = do
        let methodBytes = httpMethodToBS customMethod
        when (BS.elem 0 methodBytes) $ throwIO $ RequestContainsNul "HTTP method"
        case maybeBytes of
            Nothing ->
                checkCurlSetup
                    [CU.exp| int {
                        (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_CUSTOMREQUEST,
                                              $bs-cstr:methodBytes)
                    } |]
            Just bytes ->
                checkCurlSetup
                    [CU.block| int {
                        CURLcode code = curl_easy_setopt($(CURL* easyPtr),
                                                         CURLOPT_POSTFIELDSIZE_LARGE,
                                                         (curl_off_t)$bs-len:bytes);
                        if (code != CURLE_OK) return (int)code;
                        code = curl_easy_setopt($(CURL* easyPtr), CURLOPT_COPYPOSTFIELDS,
                                                $bs-ptr:bytes);
                        if (code != CURLE_OK) return (int)code;
                        return (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_CUSTOMREQUEST,
                                                     $bs-cstr:methodBytes);
                    } |]

setUserOptions :: (MonadIO m) => (Foldable t) => t SomeOption -> CurlEasy -> m ()
setUserOptions extraOptions (CurlEasy easyPtr) =
    liftIO $ traverse_ (setSomeOption easyPtr) extraOptions

setDefaultHTTPOptions :: (MonadIO m) => CurlEasy -> m ()
setDefaultHTTPOptions (CurlEasy easyPtr) =
    checkCurlSetup
        [CU.block| int {
            CURL *easy = $(CURL* easyPtr);
            CURLcode code;
#if LIBCURL_VERSION_NUM >= 0x071506
            code = curl_easy_setopt(easy, CURLOPT_ACCEPT_ENCODING, "");
#else
            code = curl_easy_setopt(easy, CURLOPT_ENCODING, "");
#endif
            if (code != CURLE_OK) return (int)code;
            code = curl_easy_setopt(easy, CURLOPT_FOLLOWLOCATION, 1L);
            if (code != CURLE_OK) return (int)code;
            return (int)curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
        } |]

setRequestOpts :: (MonadIO m) => Request -> CurlEasy -> m ()
setRequestOpts Request{..} (CurlEasy easyPtr) =
    checkCurlSetup
        [CU.block| int {
            CURL *easy = $(CURL* easyPtr);
            CURLcode code = curl_easy_setopt(easy, CURLOPT_URL, $bs-cstr:url);
            if (code != CURLE_OK) return (int)code;
            code = curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, $(long timeoutMS'));
            if (code != CURLE_OK) return (int)code;
            code = curl_easy_setopt(easy, CURLOPT_CONNECTTIMEOUT_MS,
                                    $(long connectionTimeoutMS'));
            if (code != CURLE_OK) return (int)code;
            code = curl_easy_setopt(easy, CURLOPT_LOW_SPEED_TIME,
                                    $(long lowSpeedTimeout'));
            if (code != CURLE_OK) return (int)code;
            return (int)curl_easy_setopt(easy, CURLOPT_LOW_SPEED_LIMIT,
                                         $(long lowSpeedLimit'));
        } |]
  where
    timeoutMS' = fromIntegral timeoutMS
    connectionTimeoutMS' = fromIntegral connectionTimeoutMS
    lowSpeedLimit' = fromIntegral lowSpeedLimit.lowSpeed
    lowSpeedTimeout' = fromIntegral lowSpeedLimit.timeout

setHeaders :: (MonadResource m, MonadUnliftIO m) => RequestBodyMode -> RequestHeader -> CurlEasy -> m [ReleaseKey]
setHeaders bodyMode headers (CurlEasy easyPtr) = mask_ $ case headers of
    NoHeaders -> setOwnedHeaders internalHeaders
    HeaderList headers' -> setOwnedHeaders $ addInternalHeaders headers'
    OverrideHeaders (CurlSlist slistFPtr)
        | bodyMode == UseStreamingRequestBody -> do
            (overlayKey, overlayPtr) <- allocateSlistOverlayHeader slistFPtr suppressExpect
            installHeaders overlayPtr `onException` release overlayKey
            pure [overlayKey]
        | otherwise -> do
            installHeadersWithForeignPtr slistFPtr
            ownerKey <- register $ touchForeignPtr slistFPtr
            pure [ownerKey]
  where
    suppressExpect = "Expect:"
    internalHeaders
        | bodyMode == UseStreamingRequestBody = [suppressExpect]
        | otherwise = []
    addInternalHeaders headers'
        | bodyMode == UseStreamingRequestBody
            && not (any hasExpectHeader headers') =
            suppressExpect : headers'
        | otherwise = headers'
    hasExpectHeader value =
        BS.length value >= BS.length suppressExpect
            && and (BS.zipWith equalAsciiCI (BS.take (BS.length suppressExpect) value) suppressExpect)
    equalAsciiCI left right = asciiLower left == asciiLower right
    asciiLower value
        | value >= 65 && value <= 90 = value + 32
        | otherwise = value

    setOwnedHeaders [] = pure []
    setOwnedHeaders headers' = do
        (releaseKeySlist, slistPtr) <- allocateSlist headers'
        installHeaders slistPtr `onException` release releaseKeySlist
        pure [releaseKeySlist]

    installHeaders slistPtr =
        checkCurlSetup
            [CU.exp| int {
                (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_HTTPHEADER,
                                      $(curl_slist_t* slistPtr))
            } |]

    installHeadersWithForeignPtr slistFPtr =
        checkCurlSetup
            [CU.exp| int {
                (int)curl_easy_setopt($(CURL* easyPtr), CURLOPT_HTTPHEADER,
                                      $fptr-ptr:(curl_slist_t* slistFPtr))
            } |]

data RequestBodyMode = UseRequestBody | UseStreamingRequestBody
    deriving (Eq)

{- | Haskell owners that must remain live while the C agent owns the easy
handle. The easy handle itself is intentionally absent: it becomes invalid
as soon as the C agent publishes completion.
-}
data RequestHandler response = RequestHandler
    { easyData :: !EasyData
    , doneRequest :: !(MVar ())
    , requestHeaders :: !HeadersData
    , responseTarget :: !response
    , metricsContext :: !CurlMetricsContext
    , resources :: ![ReleaseKey]
    }

initRequestWith :: (MonadResource m, MonadUnliftIO m) => (CurlEasy -> HeadersData -> m (response, [ReleaseKey])) -> Request -> CurlEasy -> m (RequestHandler response)
initRequestWith setResponseTarget request easy =
    fst
        <$> initRequestWithStreams
            UseRequestBody
            ( \easyHandle headersData _metricsContext -> do
                (response, resources) <- setResponseTarget easyHandle headersData
                pure (response, resources, noTransferStreams)
            )
            request
            easy

initRequestWithStreams :: (MonadResource m, MonadUnliftIO m) => RequestBodyMode -> (CurlEasy -> HeadersData -> CurlMetricsContext -> m (response, [ReleaseKey], TransferStreams)) -> Request -> CurlEasy -> m (RequestHandler response, TransferStreams)
initRequestWithStreams bodyMode setResponseTarget request@Request{..} easy = mask_ do
    setDefaultHTTPOptions easy
    setRequestOpts request easy
    headerData <- setHeaderReader easy
    metricsContext <- setMetrics
    releaseKeySlist <- setHeaders bodyMode headers easy
    let continueSetup = do
            (doneRequest, easyData) <- setEasyData
            setRequestMethodAndBody bodyMode request easy
            setUserOptions extraOptions easy
            (responseTarget, responseResources, transferStreams) <- setResponseTarget easy headerData metricsContext
            pure
                ( RequestHandler
                    { easyData
                    , doneRequest
                    , responseTarget
                    , metricsContext
                    , resources = responseResources <> releaseKeySlist
                    , requestHeaders = headerData
                    }
                , transferStreams
                )
    continueSetup `onException` traverse_ release releaseKeySlist

initRequest :: (MonadResource m, MonadUnliftIO m) => Request -> CurlEasy -> m (RequestHandler SimpleStringPtr)
initRequest = initRequestWith \easy _headers -> do
    responseSimpleString <- setSimpleStringResponse easy
    pure (responseSimpleString, [])
