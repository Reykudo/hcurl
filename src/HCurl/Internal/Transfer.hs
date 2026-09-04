{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnboxedTuples #-}

module HCurl.Internal.Transfer where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (unless, void)
import Control.Monad.Trans.Resource
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef)
import GHC.Exts (touch#)
import GHC.IO (IO (IO))
import HCurl.Internal.Agent
import HCurl.Internal.Easy
import HCurl.Internal.Once (newOnce)
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Headers (HeadersData)
import HCurl.Internal.Raw.MPSC
import HCurl.Internal.Raw.Metrics (CurlMetricsContext)
import HCurl.Request
import UnliftIO (liftIO, withRunInIO)
import UnliftIO qualified as U

data RunningTransfer response = RunningTransfer
    { transferId :: !TransferId
    , transferAgent :: !AgentHandle
    , transferHandler :: !(RequestHandler response)
    , finishTransfer :: !(IO ())
    , closeTransfer :: !(IO ())
    }

releaseAll :: [IO ()] -> IO ()
releaseAll actions = go actions Nothing
  where
    go [] Nothing = pure ()
    go [] (Just exception) = throwIO exception
    go (action : rest) firstFailure = do
        outcome <- try @SomeException action
        go rest $ case (firstFailure, outcome) of
            (Nothing, Left exception) -> Just exception
            _ -> firstFailure

-- Keep every Haskell owner of a pointer borrowed by libcurl live until the C
-- agent has completed (and stopped dereferencing) the transfer.
touchValue :: a -> IO ()
touchValue value = IO \state -> case touch# value state of
    state' -> (# state', () #)

cancelAndWait :: AgentContext -> TransferId -> MVar () -> IO ()
cancelAndWait context identifier completion = do
    requestCancel context identifier completion
    readMVar completion

requestCancel :: AgentContext -> TransferId -> MVar () -> IO ()
requestCancel context identifier completion = retryCancel
  where
    retryCancel =
        tryReadMVar completion >>= \case
            Just () -> pure ()
            Nothing ->
                sendMessage context (CancelRequest identifier)
                    `catches` [ Handler \QueueFull -> threadDelay 50 >> retryCancel
                              , Handler \MessageAllocationFailed -> threadDelay 50 >> retryCancel
                              , Handler \AgentClosed -> pure ()
                              ]

startTransferWith :: (MonadResource m, MonadUnliftIO m) => RequestBodyMode -> Agent -> Request -> (AgentContext -> TransferId -> CurlEasy -> HeadersData -> CurlMetricsContext -> m (response, [ReleaseKey], TransferStreams)) -> m (RunningTransfer response)
startTransferWith bodyMode agent request setResponseTarget = U.mask \_restore -> do
    liftIO $ validateRequest bodyMode request
    lease <- liftIO $ acquireLease agent
    leaseReleased <- liftIO $ newIORef False
    let releaseLease = do
            wasReleased <- atomicModifyIORef' leaseReleased (True,)
            unless wasReleased lease.leaseDone
        agentHandle = lease.leaseAgentHandle
        context = agentHandle.agentContext

    identifier <- liftIO (newTransferId context) `U.onException` liftIO releaseLease
    easy <- liftIO newEasy `U.onException` liftIO releaseLease
    (handler, streams) <-
        initRequestWithStreams
            bodyMode
            (setResponseTarget context identifier)
            request
            easy
            `U.onException` (liftIO (cleanupEasy easy) >> liftIO releaseLease)

    releaseHandlerResources <- withRunInIO \runInIO ->
        pure . runInIO $ traverse_ release handler.resources
    let CurlEasy easyPtr = easy
        releaseUnsubmitted =
            releaseAll
                [ cleanupEasy easy
                , releaseHandlerResources
                , releaseLease
                ]

    (cancelOnce, finishOnce) <-
        liftIO
            ( do
                cancel <- newOnce $ requestCancel context identifier handler.doneRequest
                finish <- newOnce $ do
                    readMVar handler.doneRequest
                    touchValue (handler, streams)
                    releaseAll
                        [ releaseHandlerResources
                        , releaseLease
                        ]
                pure (cancel, finish)
            )
            `U.onException` liftIO releaseUnsubmitted
    let closeOnce = cancelOnce >> finishOnce

    let submit =
            sendMessage context (Execute identifier easyPtr handler.easyData handler.metricsContext streams)
                `catch` \QueueFull -> threadDelay 50 >> submit
    liftIO submit
        `U.onException` liftIO releaseUnsubmitted

    cleanupKey <- register closeOnce `U.onException` liftIO closeOnce
    unregisterCleanup <- withRunInIO \runInIO ->
        pure . void . runInIO $ unprotect cleanupKey
    let finishTransfer = mask_ $ finishOnce >> unregisterCleanup
        closeTransfer = mask_ $ closeOnce >> unregisterCleanup
    pure RunningTransfer{transferId = identifier, transferAgent = agentHandle, transferHandler = handler, finishTransfer, closeTransfer}
