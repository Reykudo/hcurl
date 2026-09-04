{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Headers where

import Control.Exception (mask_, onException, throwIO)
import Control.Monad (unless, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Unsafe qualified as BSU
import Data.CaseInsensitive qualified as CI
import Data.Maybe (mapMaybe)
import Foreign
import Foreign.C.Types
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Headers
import HCurl.Internal.Waiter
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU
import Network.HTTP.Types.Header
import UnliftIO (MonadIO, liftIO)

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<curl/curl.h>"
C.include "headers.h"

finalizerHeadersData :: FunPtr (Ptr HeadersData -> IO ())
finalizerHeadersData =
    [C.funPtr| void hcurl_headers_finalizer(header_data_t *headers) {
        free_header_data(headers);
    } |]

setHeaderReader :: (MonadIO m) => CurlEasy -> m HeadersData
setHeaderReader (CurlEasy easyPtr) = do
    ptr <- liftIO [CU.exp| header_data_t* { header_data_create(1024) } |]
    when (ptr == nullPtr) . liftIO . throwIO $
        userError "hcurl: unable to allocate response header buffer"
    headers <-
        liftIO . mask_ $
            (HeadersData <$> newForeignPtr finalizerHeadersData ptr)
                `onException` [CU.block| void { free_header_data($(header_data_t* ptr)); } |]
    code <- liftIO $ withHeadersData headers \headersPtr ->
        [CU.exp| int {
            (int)header_data_install($(CURL* easyPtr), $(header_data_t* headersPtr))
        } |]
    unless (code == 0) . liftIO . throwIO $
        (toEnum (fromIntegral code) :: CurlCode)
    pure headers

parseHeaders :: ByteString -> [Header]
parseHeaders = mapMaybe parseHeader . BSC.lines

parseHeader :: ByteString -> Maybe Header
parseHeader line =
    case BSC.break (== ':') line of
        (key, rest)
            | BS.null key || BS.null rest -> Nothing
            | otherwise ->
                Just (CI.mk $ BSC.strip key, BSC.strip $ BS.drop 1 rest)

extractHeaderBlock :: HeadersData -> IO (Int, [Header])
extractHeaderBlock headers = do
    (statusCode, responseHeaders, _terminalResult) <- extractHeaderBlockState headers
    pure (statusCode, responseHeaders)

extractHeaderBlockState :: HeadersData -> IO (Int, [Header], Maybe CurlCode)
extractHeaderBlockState headers =
    mask_ $ withHeadersData headers \headersPtr ->
        alloca \bufferPtr ->
            alloca \lengthPtr ->
                alloca \statusPtr ->
                    alloca \terminalBeforeResponsePtr ->
                        alloca \terminalCodePtr -> do
                            success <-
                                [CU.block| int {
                                    return header_data_snapshot(
                                        $(header_data_t* headersPtr),
                                        $(char** bufferPtr),
                                        $(size_t* lengthPtr),
                                        $(long* statusPtr),
                                        $(int* terminalBeforeResponsePtr),
                                        $(int* terminalCodePtr)) ? 1 : 0;
                                } |]
                            if success == 0
                                then throwIO $ userError "hcurl: unable to snapshot response headers"
                                else do
                                    buffer <- peek bufferPtr
                                    lengthInBytes <- peek lengthPtr
                                    statusCode <- fromIntegral <$> (peek statusPtr :: IO CLong)
                                    terminalBeforeResponse <-
                                        (/= 0) <$> (peek terminalBeforeResponsePtr :: IO CInt)
                                    terminalCode <- peek terminalCodePtr
                                    if toInteger lengthInBytes > toInteger (maxBound :: Int)
                                        then free buffer >> throwIO (userError "hcurl: response headers exceed Haskell Int")
                                        else do
                                            bytes <-
                                                BSU.unsafePackMallocCStringLen (buffer, fromIntegral lengthInBytes)
                                                    `onException` free buffer
                                            let terminalResult =
                                                    if terminalBeforeResponse
                                                        then Just . toEnum $ fromIntegral (terminalCode :: CInt)
                                                        else Nothing
                                            pure (statusCode, parseHeaders bytes, terminalResult)

extractHeaders :: HeadersData -> IO [Header]
extractHeaders = fmap snd . extractHeaderBlock

{- | Wait until the final-looking response header block is available.  The C
callback only posts a one-shot MVar wake-up; it never enters Haskell.
-}
awaitHeaderBlock :: HeadersData -> IO ()
awaitHeaderBlock headers = do
    status <- awaitOneShot register unregister
    case status of
        WaitReady -> pure ()
        WaitInvalid -> throwIO (userError "hcurl: concurrent response-head waiter")
        WaitRetry -> throwIO (userError "hcurl: invalid response-head waiter retry")
        WaitRegistered -> throwIO (userError "hcurl: invalid waiter ownership state")
  where
    register stablePtr capabilityC =
        withHeadersData headers \headersPtr -> do
            rawStatus <-
                [CU.exp| int {
                    (int)header_data_wait(
                        $(header_data_t* headersPtr),
                        (HsStablePtr)$(void* stablePtr),
                        $(int capabilityC))
                } |]
            pure $ case toEnum (fromIntegral rawStatus) :: HeaderWaitResult of
                HeaderWaitRegistered -> WaitRegistered
                HeaderWaitReady -> WaitReady
                HeaderWaitInvalid -> WaitInvalid

    unregister stablePtr =
        withHeadersData headers \headersPtr ->
            (/= 0)
                <$> [CU.exp| int {
                    header_data_cancel_wait(
                        $(header_data_t* headersPtr),
                        (HsStablePtr)$(void* stablePtr)) ? 1 : 0
                } |]
