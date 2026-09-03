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
)
where

import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar (readMVar, tryReadMVar)
import Control.Concurrent.STM
import Control.Monad (void, when)
import Data.Foldable (traverse_)
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
import UnliftIO (MonadUnliftIO, finally, liftIO, mask, onException, tryAny, withRunInIO)
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
    liftIO (sendMessage agentHandle.agentContext $ Execute easyPtr)
        `onException` releaseSetup

    workerProcessed <- liftIO newEmptyTMVarIO
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
            `onException` liftIO (cancelRequest agentHandle.agentContext requestHandler)

    let detachTransfer = do
            requestDone <- tryReadMVar requestHandler.doneRequest
            when (isNothing requestDone) $ cancelRequest agentHandle.agentContext requestHandler
            readMVar requestHandler.doneRequest
            atomically $ readTMVar workerProcessed
    releaseKeyTransfer <- register detachTransfer `onException` liftIO detachTransfer
    releaseAll <- withRunInIO \runInIO ->
        pure . runInIO $ do
            release releaseKeyTransfer
            releaseSetup

    bodyReader <- liftIO $ mkBodyReader streamState (resumeRequest agentHandle.agentContext easy) releaseAll
    startResult <- restore (liftIO $ awaitBodyStart streamState) `onException` liftIO releaseAll
    case startResult of
        Left curlCode -> liftIO releaseAll >> pure (Left curlCode)
        Right responseHead ->
            pure . Right $
                StreamingResponse
                    { info = responseHead
                    , body = bodyReader
                    , completion = awaitBodyCompletion streamState `finally` releaseAll
                    }
