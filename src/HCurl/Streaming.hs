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

import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar (readMVar, tryReadMVar)
import Control.Concurrent.STM
import Control.Monad (unless, void, when)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef)
import Data.Maybe (isNothing)
import HCurl.Agent
import HCurl.Internal.Body
import HCurl.Internal.Easy
import HCurl.Internal.Metrics (extractMetrics)
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Extras (getCurlCode)
import HCurl.Internal.Raw.MPSC (OuterMessage (Execute))
import HCurl.Internal.Response (getHttpParts)
import HCurl.Request
import HCurl.Response
import UnliftIO (MonadUnliftIO, finally, liftIO, mask, onException, throwIO, tryAny, withRunInIO)
import UnliftIO.Resource

httpStreaming :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> m (Either CurlCode (StreamingResponse BodyReader))
httpStreaming = httpStreamingWith defaultStreamConfig

httpStreamingWith :: (MonadResource m, MonadUnliftIO m) => StreamConfig -> Agent -> Request -> m (Either CurlCode (StreamingResponse BodyReader))
httpStreamingWith streamConfig agent request = mask \restore -> do
    streamState <- liftIO $ newBodyStreamState streamConfig
    (releaseKeyEasy, easy) <- allocateEasy
    requestHandler <-
        initRequestWith
            ( \easyHandle headerData ->
                installBodyStream easyHandle streamState $ getHttpParts easyHandle headerData
            )
            request
            easy
    lease <- liftIO $ acquireLease agent
    let CurlEasy easyPtr = easy
        agentHandle = lease.leaseAgentHandle
        releaseSetup = do
            traverse_ release requestHandler.resources
            release releaseKeyEasy
            liftIO lease.leaseDone

    workerProcessed <- liftIO newEmptyTMVarIO
    releasedFlag <- liftIO $ newIORef False

    -- Every release path (EOF read, 'closeBody', completion, send failure and
    -- the outer resource scope) funnels through 'releaseOnce': a single
    -- idempotent teardown that first marks the stream closed (waking blocked
    -- readers and making any later read deterministically fail), then cancels
    -- the transfer if it is still running and frees all resources.
    let detachIO = do
            requestDone <- tryReadMVar requestHandler.doneRequest
            when (isNothing requestDone) $ cancelRequest agentHandle.agentContext requestHandler
            readMVar requestHandler.doneRequest
            atomically $ readTMVar workerProcessed
    releaseOnceIO <-
        withRunInIO \runInIO ->
            pure . runInIO $ do
                alreadyReleased <- liftIO $ atomicModifyIORef' releasedFlag (\wasReleased -> (True, wasReleased))
                unless alreadyReleased do
                    liftIO $ markBodyStreamClosed streamState
                    liftIO detachIO
                    releaseSetup

    _completionWorker <-
        liftIO
            ( Async.async do
                outcome <- tryAny do
                    readMVar requestHandler.doneRequest
                    getCurlCode requestHandler.easyData >>= \case
                        Ok -> do
                            responseHead <- getHttpParts requestHandler.easy requestHandler.requestHeaders
                            metrics <- extractMetrics requestHandler.metricsContext
                            pure . Right $ (responseHead, metrics)
                        curlCode -> pure $ Left curlCode
                finishBodyStream streamState outcome
                    `finally` atomically (void $ tryPutTMVar workerProcessed ())
            )
            `onException` (liftIO (cancelRequest agentHandle.agentContext requestHandler) >> releaseSetup)
    liftIO (sendMessage agentHandle.agentContext $ Execute easyPtr)
        `onException` liftIO releaseOnceIO
    _releaseKeyAll <- register releaseOnceIO `onException` liftIO releaseOnceIO

    bodyReader <- liftIO $ mkBodyReader streamState (resumeRequest agentHandle.agentContext easy) releaseOnceIO
    startResult <- restore (liftIO $ awaitBodyStart streamState) `onException` liftIO releaseOnceIO
    case startResult of
        Left curlCode -> liftIO releaseOnceIO >> pure (Left curlCode)
        Right responseHead ->
            pure . Right $
                StreamingResponse
                    { info = responseHead
                    , body = bodyReader
                    , completion = awaitBodyCompletion streamState `finally` releaseOnceIO
                    }

{- | Consume a streaming response inside a guaranteed scope. The body is
closed when the callback returns (fully read or not) and on exceptions, so
an abandoned read cannot leak the transfer.
-}
withHttpStreaming :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> (StreamingResponse BodyReader -> m a) -> m (Either CurlCode a)
withHttpStreaming = withHttpStreamingWith defaultStreamConfig

withHttpStreamingWith :: (MonadResource m, MonadUnliftIO m) => StreamConfig -> Agent -> Request -> (StreamingResponse BodyReader -> m a) -> m (Either CurlCode a)
withHttpStreamingWith streamConfig agent request action = do
    result <- httpStreamingWith streamConfig agent request
    case result of
        Left curlCode -> pure $ Left curlCode
        Right response -> do
            outcome <- tryAny $ action response
            liftIO $ closeBody response.body
            either throwIO (pure . Right) outcome
