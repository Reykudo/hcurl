{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Body (
    BodyReader,
    BodyStreamState,
    InvalidStreamBufferSize (..),
    StreamConfig (..),
    StreamingUploadUnsupported (..),
    UploadStreamState,
    bodyTransferStreams,
    closeBody,
    defaultStreamConfig,
    finishUploadBody,
    installBodyStream,
    installUploadStream,
    markBodyStreamClosed,
    markUploadClosed,
    mkBodyReader,
    newBodyStreamState,
    newUploadBodyState,
    readBody,
    streamingUploadSupported,
    uploadTransferStreams,
    validateStreamBufferSize,
    writeUploadChunk,
)
where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Resource (MonadResource, ReleaseKey)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Unsafe qualified as BSU
import Data.Functor (($>))
import Foreign (
    FunPtr,
    Ptr,
    alloca,
    castPtr,
    newForeignPtr,
    nullPtr,
    peek,
 )
import Foreign.C.Types
import Foreign.Marshal.Alloc (free)
import HCurl.Internal.Agent (AgentContext (..))
import HCurl.Internal.Once (newOnce)
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Headers (HeadersData, withHeadersData)
import HCurl.Internal.Raw.MPSC (TransferId (..), TransferStreams (..), withMessageSender)
import HCurl.Internal.Raw.Metrics (CurlMetricsContext, withCurlMetricsContext)
import HCurl.Internal.Raw.Stream
import HCurl.Internal.Waiter
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU
import Numeric.Natural (Natural)

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<curl/curl.h>"
C.include "<stdint.h>"
C.include "HsFFI.h"
C.include "stream.h"

newtype StreamConfig = StreamConfig
    { bufferedChunks :: Natural
    }
    deriving (Show, Eq)

defaultStreamConfig :: StreamConfig
defaultStreamConfig = StreamConfig{bufferedChunks = 16}

data InvalidStreamBufferSize = InvalidStreamBufferSize
    deriving (Show)

instance Exception InvalidStreamBufferSize

data StreamAllocationFailed = StreamAllocationFailed
    deriving (Show)

instance Exception StreamAllocationFailed

data StreamingUploadUnsupported = StreamingUploadUnsupported
    deriving (Show)

instance Exception StreamingUploadUnsupported

newtype BodyStreamState = BodyStreamState
    { bodyStream :: CurlStream
    }

data BodyReader = BodyReader
    { streamState :: !BodyStreamState
    , finishTransfer :: !(IO ())
    , cancelTransfer :: !(IO ())
    , readLock :: !(MVar ())
    }

data UploadStreamState = UploadStreamState
    { rawUploadStream :: !CurlStream
    , uploadLock :: !(MVar ())
    }

finalizerCurlStream :: FunPtr (Ptr CurlStream -> IO ())
finalizerCurlStream =
    [C.funPtr| void hcurl_stream_finalizer(hcurl_stream_t *stream) {
        hcurl_stream_destroy(stream);
    } |]

validateStreamBufferSize :: Natural -> IO ()
validateStreamBufferSize requestedCapacity
    | requestedCapacity == 0 = throwIO InvalidStreamBufferSize
    | toInteger requestedCapacity > toInteger (maxBound :: CSize) =
        throwIO InvalidStreamBufferSize
    | otherwise = pure ()

newCurlStream :: StreamKind -> Natural -> AgentContext -> TransferId -> IO CurlStream
newCurlStream kind requestedCapacity context (TransferId identifier) = mask_ do
    validateStreamBufferSize requestedCapacity
    let capacity = fromIntegral requestedCapacity :: CSize
        kindC = fromIntegral (fromEnum kind) :: CInt
    ptr <- withMessageSender context.messageSender \senderPtr ->
        [CU.exp| hcurl_stream_t* {
            hcurl_stream_create(
                (enum hcurl_stream_kind)$(int kindC),
                $(size_t capacity),
                $(message_sender_t* senderPtr),
                (transfer_id_t)$(uint64_t identifier))
        } |]
    if ptr == nullPtr
        then throwIO StreamAllocationFailed
        else
            (CurlStream <$> newForeignPtr finalizerCurlStream ptr)
                `onException` [CU.block| void { hcurl_stream_destroy($(hcurl_stream_t* ptr)); } |]

newBodyStreamState :: StreamConfig -> AgentContext -> TransferId -> IO BodyStreamState
newBodyStreamState StreamConfig{bufferedChunks} context transferId = do
    bodyStream <- newCurlStream StreamDownload bufferedChunks context transferId
    pure BodyStreamState{..}

newUploadBodyState :: Natural -> AgentContext -> TransferId -> IO UploadStreamState
newUploadBodyState capacity context transferId = do
    rawUploadStream <- newCurlStream StreamUpload capacity context transferId
    uploadLock <- newMVar ()
    pure UploadStreamState{..}

bodyTransferStreams :: BodyStreamState -> TransferStreams
bodyTransferStreams BodyStreamState{bodyStream} = TransferStreams (Just bodyStream) Nothing

uploadTransferStreams :: UploadStreamState -> TransferStreams
uploadTransferStreams UploadStreamState{rawUploadStream} = TransferStreams Nothing (Just rawUploadStream)

installBodyStream :: (MonadResource m) => CurlEasy -> HeadersData -> CurlMetricsContext -> BodyStreamState -> m [ReleaseKey]
installBodyStream (CurlEasy easyPtr) headers metrics BodyStreamState{bodyStream} = do
    code <- liftIO $ withCurlStream bodyStream \streamPtr ->
        withHeadersData headers \headersPtr ->
            withCurlMetricsContext metrics \metricsPtr ->
                [CU.exp| int {
                    (int)hcurl_stream_install_download(
                        $(CURL* easyPtr), $(hcurl_stream_t* streamPtr),
                        $(header_data_t* headersPtr),
                        $(curl_metrics_context_t* metricsPtr))
                } |]
    unless (code == 0) . liftIO . throwIO $ decodeCurlCode code
    pure []

installUploadStream :: (MonadResource m) => CurlEasy -> CurlMetricsContext -> UploadStreamState -> m [ReleaseKey]
installUploadStream (CurlEasy easyPtr) metrics UploadStreamState{rawUploadStream} = do
    code <- liftIO $ withCurlStream rawUploadStream \streamPtr ->
        withCurlMetricsContext metrics \metricsPtr ->
            [CU.exp| int {
                (int)hcurl_stream_install_upload(
                    $(CURL* easyPtr), $(hcurl_stream_t* streamPtr),
                    $(curl_metrics_context_t* metricsPtr))
            } |]
    liftIO $ case code of
        0 -> pure ()
        unsupported
            | unsupported == streamUploadUnsupported ->
                throwIO StreamingUploadUnsupported
        _ -> throwIO $ decodeCurlCode code
    pure []

{- | Whether libcurl has a correct event-driven upload pause implementation.
Version 7.18.x exposes CURL_READFUNC_PAUSE but implements it unsafely; this
was fixed in 7.19.0.
-}
streamingUploadSupported :: IO Bool
streamingUploadSupported =
    (/= 0) <$> [CU.exp| int { hcurl_stream_upload_supported() ? 1 : 0 } |]

flushResume :: CurlStream -> IO ()
flushResume stream = do
    status <- withCurlStream stream \streamPtr ->
        [CU.exp| int { (int)hcurl_stream_flush_resume($(hcurl_stream_t* streamPtr)) } |]
    case decodeEnum status :: StreamControlResult of
        StreamControlOk -> pure ()
        StreamControlRetry -> threadDelay 50 >> flushResume stream
        StreamControlClosed -> pure ()

data WaitKind = WaitForRead | WaitForWrite

waitForStream :: WaitKind -> CurlStream -> IO ()
waitForStream waitKind stream = do
    status <- awaitOneShot register unregister
    case status of
        WaitReady -> pure ()
        WaitRetry -> flushResume stream >> waitForStream waitKind stream
        WaitInvalid -> throwIO (userError "hcurl: concurrent stream waiter")
        WaitRegistered -> throwIO (userError "hcurl: invalid waiter ownership state")
  where
    register stablePtr capabilityC =
        withCurlStream stream \streamPtr -> do
            rawStatus <- case waitKind of
                WaitForRead ->
                    [CU.exp| int {
                        (int)hcurl_stream_wait_read(
                            $(hcurl_stream_t* streamPtr),
                            (HsStablePtr)$(void* stablePtr),
                            $(int capabilityC))
                    } |]
                WaitForWrite ->
                    [CU.exp| int {
                        (int)hcurl_stream_wait_write(
                            $(hcurl_stream_t* streamPtr),
                            (HsStablePtr)$(void* stablePtr),
                            $(int capabilityC))
                    } |]
            pure $ case decodeEnum rawStatus :: StreamWaitResult of
                StreamWaitRegistered -> WaitRegistered
                StreamWaitReady -> WaitReady
                StreamWaitControlPending -> WaitRetry
                StreamWaitInvalid -> WaitInvalid

    unregister stablePtr =
        withCurlStream stream \streamPtr ->
            (/= 0) <$> case waitKind of
                WaitForRead ->
                    [CU.exp| int {
                        hcurl_stream_cancel_wait_read(
                            $(hcurl_stream_t* streamPtr),
                            (HsStablePtr)$(void* stablePtr)) ? 1 : 0
                    } |]
                WaitForWrite ->
                    [CU.exp| int {
                        hcurl_stream_cancel_wait_write(
                            $(hcurl_stream_t* streamPtr),
                            (HsStablePtr)$(void* stablePtr)) ? 1 : 0
                    } |]

mkBodyReader :: BodyStreamState -> IO () -> IO () -> IO BodyReader
mkBodyReader streamState finishTransfer closeTransfer = do
    readLock <- newMVar ()
    cancelTransfer <- newOnce closeTransfer
    pure BodyReader{..}

readBody :: BodyReader -> IO (Either CurlCode (Maybe ByteString))
readBody BodyReader{streamState = BodyStreamState{bodyStream}, finishTransfer, readLock} =
    withMVar readLock $ const $ mask_ readLoop
  where
    readLoop = do
        result <-
            withCurlStream bodyStream \streamPtr ->
                alloca \(dataPtr :: Ptr (Ptr ())) ->
                    alloca \(lengthPtr :: Ptr CSize) ->
                        alloca \(codePtr :: Ptr CInt) -> do
                            status <-
                                [CU.block| int {
                                    return (int)hcurl_stream_try_read(
                                        $(hcurl_stream_t* streamPtr),
                                        $(void** dataPtr),
                                        $(size_t* lengthPtr),
                                        $(int* codePtr));
                                } |]
                            dataAddress <- peek dataPtr
                            lengthInBytes <- peek lengthPtr
                            code <- peek codePtr
                            pure (decodeEnum status :: StreamReadResult, dataAddress, lengthInBytes, code)
        case result of
            (StreamReadChunk, dataAddress, lengthInBytes, _) -> do
                chunk <- takeChunk dataAddress lengthInBytes
                pure . Right $ Just chunk
            (StreamReadWouldBlock, _, _, _) -> waitForStream WaitForRead bodyStream >> readLoop
            (StreamReadEof, _, _, _) -> finishTransfer $> Right Nothing
            (StreamReadError, _, _, code) -> finishTransfer $> Left (decodeCurlCode code)
            (StreamReadClosed, _, _, _) -> pure $ Left AbortedByCallback
            (StreamReadChunkControlPending, dataAddress, lengthInBytes, _) -> do
                chunk <- takeChunk dataAddress lengthInBytes
                flushResume bodyStream
                pure . Right $ Just chunk

    takeChunk dataAddress lengthInBytes =
        if toInteger lengthInBytes > toInteger (maxBound :: Int)
            then free dataAddress >> throwIO (userError "hcurl: response chunk exceeds Haskell Int")
            else
                BSU.unsafePackMallocCStringLen (castPtr dataAddress, fromIntegral lengthInBytes)
                    `onException` free dataAddress

decodeCurlCode :: CInt -> CurlCode
decodeCurlCode = toEnum . fromIntegral

decodeEnum :: (Enum value) => CInt -> value
decodeEnum = toEnum . fromIntegral

closeRawStream :: CurlStream -> IO ()
closeRawStream stream = do
    status <- withCurlStream stream \streamPtr ->
        [CU.exp| int { (int)hcurl_stream_close($(hcurl_stream_t* streamPtr)) } |]
    case decodeEnum status :: StreamControlResult of
        StreamControlOk -> pure ()
        StreamControlRetry -> threadDelay 50 >> closeRawStream stream
        StreamControlClosed -> pure ()

markBodyStreamClosed :: BodyStreamState -> IO ()
markBodyStreamClosed BodyStreamState{bodyStream} = closeRawStream bodyStream

closeBody :: BodyReader -> IO ()
closeBody BodyReader{cancelTransfer} = mask_ cancelTransfer

writeUploadChunk :: UploadStreamState -> ByteString -> IO (Either CurlCode ())
writeUploadChunk UploadStreamState{rawUploadStream, uploadLock} bytes =
    withMVar uploadLock $
        const $
            BS.useAsCStringLen bytes \(source, lengthInBytes) ->
                writeLoop source (fromIntegral lengthInBytes)
  where
    writeLoop source lengthC = do
        (status, code) <-
            withCurlStream rawUploadStream \streamPtr ->
                alloca \(codePtr :: Ptr CInt) -> do
                    status <-
                        [CU.block| int {
                            return (int)hcurl_stream_try_write(
                                $(hcurl_stream_t* streamPtr),
                                $(char* source),
                                $(size_t lengthC),
                                $(int* codePtr));
                        } |]
                    (status,) <$> peek codePtr
        case decodeEnum status :: StreamWriteResult of
            StreamWriteOk -> pure $ Right ()
            StreamWriteWouldBlock -> waitForStream WaitForWrite rawUploadStream >> writeLoop source lengthC
            StreamWriteTerminal ->
                pure . Left $
                    case decodeCurlCode code of
                        Ok -> AbortedByCallback
                        curlCode -> curlCode
            StreamWriteClosed -> pure $ Left AbortedByCallback
            StreamWriteControlPending -> flushResume rawUploadStream $> Right ()
            StreamWriteOutOfMemory -> pure $ Left OutOfMemory

finishUploadBody :: UploadStreamState -> IO (Either CurlCode ())
finishUploadBody UploadStreamState{rawUploadStream, uploadLock} =
    withMVar uploadLock $ const finishLoop
  where
    finishLoop = do
        (status, code) <-
            withCurlStream rawUploadStream \streamPtr ->
                alloca \(codePtr :: Ptr CInt) -> do
                    status <-
                        [CU.block| int {
                                return (int)hcurl_stream_finish_upload(
                                    $(hcurl_stream_t* streamPtr),
                                    $(int* codePtr));
                        } |]
                    (status,) <$> peek codePtr
        case decodeEnum status :: StreamWriteResult of
            StreamWriteOk -> pure $ Right ()
            StreamWriteTerminal ->
                pure $
                    case decodeCurlCode code of
                        Ok -> Right ()
                        curlCode -> Left curlCode
            StreamWriteClosed -> pure $ Left AbortedByCallback
            StreamWriteControlPending -> flushResume rawUploadStream >> finishLoop
            StreamWriteWouldBlock -> waitForStream WaitForWrite rawUploadStream >> finishLoop
            StreamWriteOutOfMemory -> pure $ Left OutOfMemory

markUploadClosed :: UploadStreamState -> IO ()
markUploadClosed UploadStreamState{rawUploadStream} = closeRawStream rawUploadStream
