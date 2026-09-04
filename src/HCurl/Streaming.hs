module HCurl.Streaming (
    BodyReader,
    InvalidStreamBufferSize (..),
    StreamConfig (..),
    StreamingResponse (..),
    closeBody,
    defaultStreamConfig,
    httpStreaming,
    httpStreamingWith,
    readBody,
    withHttpStreaming,
    withHttpStreamingWith,
)
where

import Control.Concurrent.MVar (readMVar)
import Control.Exception (mask_)
import Control.Monad (void)
import HCurl.Agent
import HCurl.Internal.Body
import HCurl.Internal.Easy
import HCurl.Internal.Headers (awaitHeaderBlock)
import HCurl.Internal.Response (getCompletedHttpParts, getHttpPartsState, getTransferResult)
import HCurl.Internal.Result (getCurlCode)
import HCurl.Internal.Transfer
import HCurl.Metrics (Metrics)
import HCurl.Request
import HCurl.Response
import HCurl.Types (CurlCode (..))
import UnliftIO (MonadUnliftIO, liftIO, mask, onException, throwIO, tryAny, withRunInIO)
import UnliftIO.Resource

httpStreaming :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> m (Either CurlCode (StreamingResponse BodyReader))
httpStreaming = httpStreamingWith defaultStreamConfig

httpStreamingWith :: (MonadResource m, MonadUnliftIO m) => StreamConfig -> Agent -> Request -> m (Either CurlCode (StreamingResponse BodyReader))
httpStreamingWith streamConfig agent request = mask \restore -> do
    liftIO $ validateStreamBufferSize streamConfig.bufferedChunks
    transfer <-
        startTransferWith UseRequestBody agent request \context identifier easy headers metrics -> do
            streamState <- liftIO $ newBodyStreamState streamConfig context identifier
            resources <- installBodyStream easy headers metrics streamState
            pure (streamState, resources, bodyTransferStreams streamState)
    let requestHandler = transfer.transferHandler
        streamState = requestHandler.responseTarget
    let cancelOnce = markBodyStreamClosed streamState >> transfer.finishTransfer
    cleanupKey <- register cancelOnce `onException` liftIO cancelOnce
    unregisterCleanup <- withRunInIO \runInIO ->
        pure $ void $ runInIO $ unprotect cleanupKey
    let finishBodyTransfer = mask_ $ transfer.finishTransfer >> unregisterCleanup
        closeBodyTransfer = mask_ $ cancelOnce >> unregisterCleanup

    bodyReader <- liftIO $ mkBodyReader streamState finishBodyTransfer closeBodyTransfer
    startResult <- restore (liftIO $ awaitResponseHead requestHandler) `onException` liftIO closeBodyTransfer
    case startResult of
        Left curlCode -> liftIO finishBodyTransfer >> pure (Left curlCode)
        Right responseHead ->
            pure . Right $
                StreamingResponse
                    { info = responseHead
                    , body = bodyReader
                    , completion = mask_ do
                        result <- awaitCompletion requestHandler
                        transfer.finishTransfer
                        pure result
                    }

awaitResponseHead :: RequestHandler response -> IO (Either CurlCode HttpParts)
awaitResponseHead requestHandler = do
    awaitHeaderBlock requestHandler.requestHeaders
    (responseHead, terminalResult) <- getHttpPartsState requestHandler.requestHeaders
    case terminalResult of
        Just curlCode | curlCode /= Ok -> pure $ Left curlCode
        _ | responseHead.statusCode /= 0 -> pure $ Right responseHead
        _ -> do
            readMVar requestHandler.doneRequest
            getCurlCode requestHandler.easyData >>= \case
                Ok -> Right <$> getCompletedHttpParts requestHandler.easyData requestHandler.requestHeaders
                curlCode -> pure $ Left curlCode

awaitCompletion :: RequestHandler response -> IO (Either CurlCode Metrics)
awaitCompletion requestHandler = do
    readMVar requestHandler.doneRequest
    getTransferResult requestHandler.easyData requestHandler.metricsContext

{- | Consume a streaming response inside a guaranteed scope. The body is
closed when the callback returns (fully read or not) and on exceptions, so
an abandoned read cannot leak the transfer.
-}
withHttpStreaming :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> (StreamingResponse BodyReader -> m a) -> m (Either CurlCode a)
withHttpStreaming = withHttpStreamingWith defaultStreamConfig

withHttpStreamingWith :: (MonadResource m, MonadUnliftIO m) => StreamConfig -> Agent -> Request -> (StreamingResponse BodyReader -> m a) -> m (Either CurlCode a)
withHttpStreamingWith streamConfig agent request action = mask \restore -> do
    result <- httpStreamingWith streamConfig agent request
    case result of
        Left curlCode -> pure $ Left curlCode
        Right response -> do
            outcome <- tryAny $ restore $ action response
            liftIO $ closeBody response.body
            either throwIO (pure . Right) outcome
