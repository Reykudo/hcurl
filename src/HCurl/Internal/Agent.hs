{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -Wno-type-defaults #-}

module HCurl.Internal.Agent where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Concurrent.Async (Async)
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (filterM, forM_, forever, unless, when)
import Data.Either (lefts)
import Data.IORef (IORef)
import Data.IORef qualified as Boxed
import Data.IORef.Unboxed (IORefU, modifyIORefU, newIORefU, readIORefU, writeIORefU)
import Data.List (minimumBy)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (catMaybes)
import Data.Ord (comparing)
import Data.RoundRobin (RoundRobin, newRoundRobin)
import Data.RoundRobin qualified as RoundRobin
import Data.Traversable
import Data.Word (Word64)
import Foreign.C.Types (CInt)
import Foreign.ForeignPtr (newForeignPtr)
import Foreign.Ptr
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Generics
import HCurl.Internal.MPSC
import HCurl.Internal.Multi
import HCurl.Internal.Once (Once, newOnceState, runOnce)
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Extras (withEasyData)
import HCurl.Internal.Raw.MPSC
import HCurl.Internal.Raw.Metrics (withCurlMetricsContext)
import HCurl.Internal.Raw.Stream (CurlStream, withCurlStream)
import HCurl.Internal.Raw.UV
import HCurl.Types
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<string.h>"
C.include "<stdlib.h>"

C.include "<curl/curl.h>"
C.include "HsFFI.h"

C.include "simple_string.h"
C.include "curl_uv.h"
C.include "message_chan.h"
C.include "include/waitfree-mpsc-queue/mpscq.h"

data AgentContext = AgentContext
    { uvLoop :: !UVLoop
    , uvAsync :: !UVAsync
    , multi :: !(Ptr CurlMulti)
    , msgQueue :: !MPSCQ
    , messageSender :: !MessageSender
    , nextId :: !(IORef Word64)
    , contextStatus :: !(MVar AgentContextStatus)
    }
    deriving (Generic)

data AgentHandle = AgentHandle
    { agentThreadId :: !(Async ())
    , agentContext :: !AgentContext
    , agentCloseState :: !(MVar Once)
    }
    deriving (Generic)

data Agent
    = Single AgentHandle
    | Threaded !(NonEmpty AgentHandle) !(RoundRobin AgentHandle) !(MVar Once)
    | Managed ManagedAgent

data AgentContextStatus
    = AgentContextRunning
    | AgentContextStopping
    | AgentContextStopped !(Either SomeException ())

data AgentClosed = AgentClosed
    deriving (Show)
    deriving anyclass (Exception)

data TransferIdExhausted = TransferIdExhausted
    deriving (Show)
    deriving anyclass (Exception)

{- | Dynamic scaling policy for the managed agent.

Growth and shrinking run on different clocks:

  * growth is decided when a request is admitted ('acquireLease'): if the
    least loaded selectable worker already carries at least @mpGrowLoad@
    active transfers, a new agent is started immediately (up to
    @mpMaxAgents@, itself capped at the number of capabilities). Sharp
    demand spikes therefore get capacity right away, without waiting for a
    controller tick. Spawns are rate limited by @mpSpawnCooldownMicros@.

  * shrinking is decided by a controller thread that samples the pool
    every @mpTickMicros@ and keeps an EWMA of the total in-flight demand
    (a "recommendation" averaged over a period). A worker is drained only
    when the smoothed per-worker demand stays at or below @mpShrinkLoad@
    for @mpSustainTicks@ consecutive ticks, and no spawn happened within
    the last @mpKillCooldownMicros@. Draining stops routing new transfers
    to the worker and tears its loop down once its in-flight transfers
    finish, so shrinking also works under sustained (but light) traffic
    and cannot race a recent spawn into an oscillation.
-}
data ManagedPolicy = ManagedPolicy
    { mpMinAgents :: !Int
    , mpMaxAgents :: !Int
    , mpTickMicros :: !Int
    , mpSustainTicks :: !Int
    , mpEwmaAlpha :: !Double
    , mpGrowLoad :: !Int
    , mpShrinkLoad :: !Double
    , mpSpawnCooldownMicros :: !Int
    , mpKillCooldownMicros :: !Int
    }
    deriving (Show, Eq)

{- | Defaults: one initial agent and at most one agent per capability,
quarter-second sampling, grow at
admission when every selectable worker already carries eight active
transfers, drain once the period-averaged demand drops below a third of a
transfer per worker. Spawns are rate limited to one per tick; kills are
held back for five seconds after the last spawn.
-}
defaultManagedPolicy :: IO ManagedPolicy
defaultManagedPolicy = do
    realCapabilities <- getNumCapabilities
    pure
        ManagedPolicy
            { mpMinAgents = 1
            , mpMaxAgents = realCapabilities
            , mpTickMicros = 250_000
            , mpSustainTicks = 4
            , mpEwmaAlpha = 0.25
            , mpGrowLoad = 8
            , mpShrinkLoad = 0.35
            , mpSpawnCooldownMicros = 250_000
            , mpKillCooldownMicros = 5_000_000
            }

-- Active count and utilization are unboxed storage, not synchronization
-- primitives. Every access is serialized by the owning ManagedAgent.maState.
data ManagedWorker = ManagedWorker
    { mwId :: !Int
    , mwHandle :: !AgentHandle
    , mwActive :: !(IORefU Int)
    , mwUtilization :: !(IORefU Double)
    , mwQuiescing :: !(IORef Bool)
    }

data ManagedState = ManagedState
    { msWorkers :: ![ManagedWorker]
    , msLoad :: !Double
    , msShrinkStreak :: !Int
    , msLastSpawn :: !Word64
    , msPolicy :: !ManagedPolicy
    , msConfig :: !AgentConfig
    , msNextId :: !Int
    , msClosed :: !Bool
    }

-- | A snapshot of the managed pool handed to the metrics hook.
data ManagedMetrics = ManagedMetrics
    { mmRunningAgents :: !Int
    , mmDemand :: !Double
    }
    deriving (Show, Eq)

{- | Callback invoked by the pool controller whenever it samples the pool.
The hook is called outside the pool lock. Ordinary exceptions it throws are
swallowed so a misbehaving metrics exporter cannot kill the controller;
asynchronous exceptions are propagated so managed shutdown remains reliable.
Wire this up to whatever metrics backend you use (e.g. Prometheus gauges).
-}
type MetricsHook = ManagedMetrics -> IO ()

data ManagedAgent = ManagedAgent
    { maState :: !(MVar ManagedState)
    , maHook :: !(IORef MetricsHook)
    , maController :: !(MVar (Async ()))
    , maCloseState :: !(MVar Once)
    }

data Lease = Lease
    { leaseAgentHandle :: !AgentHandle
    , leaseDone :: !(IO ())
    }

spawnThreadedAgent :: Int -> AgentConfig -> IO Agent
spawnThreadedAgent numThreads config = mask_ do
    realCapabilities <- getNumCapabilities
    let agentCount = max 1 (min realCapabilities numThreads)
    handlesList <- spawnHandles config [0 .. agentCount - 1]
    handles <- case NonEmpty.nonEmpty handlesList of
        Nothing -> throwIO $ userError "hcurl: threaded agent has no workers"
        Just nonEmptyHandles -> pure nonEmptyHandles
    rr <- newRoundRobin handles `onException` releaseAllAgents handlesList
    closeState <- newOnceState `onException` releaseAllAgents handlesList
    pure $ Threaded handles rr closeState

{- | Start a demand-driven pool of agents.

The pool starts with @mpMinAgents@ workers. Growth happens synchronously
when requests are admitted; shrinking is handled by a controller thread
from the period-averaged demand (see @ManagedPolicy@). Draining never
interrupts in-flight transfers: a draining worker stops receiving new
requests and its loop is torn down only once it has no active transfers
left.
-}
spawnManagedAgent :: ManagedPolicy -> AgentConfig -> IO Agent
spawnManagedAgent rawPolicy config = mask_ do
    realCapabilities <- getNumCapabilities
    let policy = clampPolicy realCapabilities rawPolicy
        startCount = policy.mpMinAgents
    handles <- spawnHandles config [0 .. startCount - 1]
    let finishSetup = do
            workers <-
                sequence
                    [ newManagedWorker index agentHandle
                    | (index, agentHandle) <- zip [0 ..] handles
                    ]
            state <- newMVar $ ManagedState workers 0 0 0 policy config startCount False
            hook <- Boxed.newIORef $ \_ -> pure ()
            controllerSlot <- newEmptyMVar
            closeState <- newOnceState
            let managedAgent = ManagedAgent state hook controllerSlot closeState
            controller <- Async.asyncWithUnmask \unmask ->
                controllerLoop unmask managedAgent policy
            putMVar controllerSlot controller
            let agent = Managed managedAgent
            runHook hook (ManagedMetrics startCount 0)
            pure agent
    finishSetup `onException` releaseAllAgents handles

controllerLoop :: (forall value. IO value -> IO value) -> ManagedAgent -> ManagedPolicy -> IO ()
controllerLoop unmask managedAgent policy = forever $ mask_ do
    unmask $ threadDelay policy.mpTickMicros
    now <- getMonotonicTimeNSec
    retiring <- modifyMVar managedAgent.maState $ \managedState -> controllerTick policy now managedState
    stateAfter <- readMVar managedAgent.maState
    unmask $ metricsOf stateAfter >>= runHook managedAgent.maHook
    retireManagedWorkers managedAgent retiring

{- | Set (or replace) the metrics hook of a 'Managed' agent. The hook is
invoked with a fresh snapshot immediately, and then after every controller
tick. 'Single' and 'Threaded' agents have no demand controller and reject
registration.
-}
registerManagedMetrics :: Agent -> MetricsHook -> IO ()
registerManagedMetrics agent hook = case agent of
    Managed managedAgent -> do
        Boxed.writeIORef managedAgent.maHook hook
        snapshot <- metricsOf =<< readMVar managedAgent.maState
        runHook managedAgent.maHook snapshot
    _ -> throwIO $ userError "metrics hook requires a Managed agent"

-- | Current snapshot: number of running agent loops and the smoothed demand.
sampleManagedMetrics :: Agent -> IO (Maybe ManagedMetrics)
sampleManagedMetrics = \case
    Single _ -> pure Nothing
    Threaded{} -> pure Nothing
    Managed managedAgent -> Just <$> (metricsOf =<< readMVar managedAgent.maState)

metricsOf :: ManagedState -> IO ManagedMetrics
metricsOf managedState = do
    running <- filterM (agentHandleRunning . mwHandle) managedState.msWorkers
    pure
        ManagedMetrics
            { mmRunningAgents = length running
            , mmDemand = managedState.msLoad
            }

runHook :: IORef MetricsHook -> ManagedMetrics -> IO ()
runHook hookRef snapshot = do
    hook <- Boxed.readIORef hookRef
    ignoreSynchronousException $ hook snapshot

ignoreSynchronousException :: IO () -> IO ()
ignoreSynchronousException action =
    action `catch` \exception ->
        case fromException exception :: Maybe SomeAsyncException of
            Just _ -> throwIO exception
            Nothing -> pure ()

newManagedWorker :: Int -> AgentHandle -> IO ManagedWorker
newManagedWorker index agentHandle =
    ManagedWorker index agentHandle <$> newIORefU 0 <*> newIORefU 0 <*> Boxed.newIORef False

workerIsQuiescing :: ManagedWorker -> IO Bool
workerIsQuiescing worker = Boxed.readIORef worker.mwQuiescing

markWorkerQuiescing :: ManagedWorker -> IO ()
markWorkerQuiescing worker = Boxed.writeIORef worker.mwQuiescing True

clampPolicy :: Int -> ManagedPolicy -> ManagedPolicy
clampPolicy realCapabilities policy =
    let maximumAgents = max 1 $ min realCapabilities (max 1 policy.mpMaxAgents)
        minimumAgents = min maximumAgents $ max 1 policy.mpMinAgents
     in ManagedPolicy
            { mpMinAgents = minimumAgents
            , mpMaxAgents = maximumAgents
            , mpTickMicros = max 10_000 policy.mpTickMicros
            , mpSustainTicks = max 1 policy.mpSustainTicks
            , mpEwmaAlpha = clampFinite 0.25 0.01 1 policy.mpEwmaAlpha
            , mpGrowLoad = max 1 policy.mpGrowLoad
            , mpShrinkLoad = clampFinite 0.35 0 (fromIntegral (maxBound :: Int)) policy.mpShrinkLoad
            , mpSpawnCooldownMicros = max 0 policy.mpSpawnCooldownMicros
            , mpKillCooldownMicros = max 0 policy.mpKillCooldownMicros
            }
  where
    clampFinite fallback lower upper value
        | isNaN value || isInfinite value = fallback
        | otherwise = max lower $ min upper value

microsToNanos :: Int -> Word64
microsToNanos microseconds =
    fromInteger . min (toInteger (maxBound :: Word64)) $
        toInteger (max 0 microseconds) * 1000

selectableWorkers :: ManagedState -> IO [ManagedWorker]
selectableWorkers managedState =
    filterM isSelectable managedState.msWorkers
  where
    isSelectable worker = do
        quiescing <- workerIsQuiescing worker
        running <- agentHandleRunning worker.mwHandle
        unless running $ markWorkerQuiescing worker
        pure $ not quiescing && running

agentHandleRunning :: AgentHandle -> IO Bool
agentHandleRunning agentHandle =
    readMVar agentHandle.agentContext.contextStatus >>= \case
        AgentContextRunning -> pure True
        AgentContextStopping -> pure False
        AgentContextStopped{} -> pure False

{- | One controller tick: sample demand, update the EWMA ("recommendation")
and decide whether to drain. Growth is handled synchronously at request
admission, never here. A drain is only started once the smoothed
per-worker demand has been at or below 'mpShrinkLoad' for
'mpSustainTicks' consecutive ticks and no spawn happened within the kill
cooldown, so fresh workers are not killed right after being started.
Returns workers whose loops should now be stopped. They remain owned by the
state until that stop has completed.
-}
controllerTick :: ManagedPolicy -> Word64 -> ManagedState -> IO (ManagedState, [ManagedWorker])
controllerTick policy now managedState
    | managedState.msClosed = pure (managedState, [])
    | otherwise = do
        refreshWorkerStates managedState.msWorkers
        actives <- forM managedState.msWorkers $ \worker -> do
            active <- readIORefU worker.mwActive
            quiescing <- workerIsQuiescing worker
            pure (worker, active, quiescing)
        let alpha = policy.mpEwmaAlpha
            selectable = [(worker, active) | (worker, active, quiescing) <- actives, not quiescing]
            workerCount = length selectable
            sampleLoad = fromIntegral (sum (map snd selectable)) :: Double
            ewmaLoad = managedState.msLoad + alpha * (sampleLoad - managedState.msLoad)
        forM_ actives $ \(worker, active, _) -> do
            current <- readIORefU worker.mwUtilization
            let busy = if active > 0 then 1 else 0 :: Double
            writeIORefU worker.mwUtilization $ current + alpha * (busy - current)
        let withLoad = managedState{msLoad = ewmaLoad}
            perWorker = if workerCount == 0 then 0 else ewmaLoad / fromIntegral workerCount
            killCooldownNs = microsToNanos policy.mpKillCooldownMicros
            canKill = now - managedState.msLastSpawn >= killCooldownNs
            readyToShrink = workerCount > policy.mpMinAgents && canKill
        decided <-
            if readyToShrink && perWorker <= policy.mpShrinkLoad
                then shrinkStep withLoad
                else pure withLoad{msShrinkStreak = 0}
        (drained, retiring) <- drainFinished decided
        restored <- restoreMinimum policy now drained
        pure (restored, retiring)
  where
    shrinkStep :: ManagedState -> IO ManagedState
    shrinkStep state = do
        let streak = state.msShrinkStreak + 1
        if streak >= policy.mpSustainTicks
            then do
                selectable <- selectableWorkers state
                ranked <- forM selectable $ \worker -> do
                    active <- readIORefU worker.mwActive
                    utilization <- readIORefU worker.mwUtilization
                    pure (worker, active, utilization)
                case ranked of
                    [] -> pure state
                    _ -> do
                        let (victim, _, _) = minimumBy (comparing (\(_, active, utilization) -> (active, utilization))) ranked
                        markWorkerQuiescing victim
                        pure state{msShrinkStreak = 0}
            else pure state{msShrinkStreak = streak}

-- A reactor can stop independently after a fatal libcurl/libuv error.  Mark
-- it as draining before accounting demand so it neither inflates metrics nor
-- remains selectable until a later admission happens to discover it.
refreshWorkerStates :: [ManagedWorker] -> IO ()
refreshWorkerStates workers =
    forM_ workers \worker -> do
        running <- agentHandleRunning worker.mwHandle
        unless running $ markWorkerQuiescing worker

-- Keep the policy's minimum available after a reactor exits unexpectedly.
-- Synchronous spawn failures are retried on the next controller tick; an
-- asynchronous exception still terminates the controller during shutdown.
restoreMinimum :: ManagedPolicy -> Word64 -> ManagedState -> IO ManagedState
restoreMinimum policy now = go
  where
    go state = do
        selectable <- selectableWorkers state
        if length selectable >= policy.mpMinAgents
            then pure state
            else
                try @SomeException (spawnManagedWorker policy state.msConfig now state) >>= \case
                    Right (_, grown) -> go grown
                    Left exception ->
                        case fromException exception :: Maybe SomeAsyncException of
                            Just _ -> throwIO exception
                            Nothing -> pure state

{- | Find draining workers whose transfers have all finished.

They deliberately remain in 'msWorkers' until 'retireManagedWorkers' has
finished stopping them. This keeps ownership visible to 'closeManagedAgent'
and to the next controller tick if the retiring thread is interrupted.
-}
drainFinished :: ManagedState -> IO (ManagedState, [ManagedWorker])
drainFinished managedState = do
    drained <- forM managedState.msWorkers $ \worker -> do
        active <- readIORefU worker.mwActive
        quiescing <- workerIsQuiescing worker
        pure (worker, active, quiescing)
    let finished = [worker | (worker, active, quiescing) <- drained, quiescing, active == 0]
    pure (managedState, finished)

spawnHandles :: AgentConfig -> [Int] -> IO [AgentHandle]
spawnHandles config = go []
  where
    go started [] = pure $ reverse started
    go started (capability : rest) = do
        agentHandle <-
            newAgentOn capability config
                `onException` releaseAllAgents started
        go (agentHandle : started) rest

newAgentHandle :: (((forall value. IO value -> IO value) -> IO ()) -> IO (Async ())) -> AgentConfig -> IO AgentHandle
newAgentHandle launch config = mask_ do
    multi <- initCurlMulti config
    agentContext <- new multi `onException` cleanupUnstartedMulti multi
    agentCloseState <- newOnceState `onException` destroyAgentContext agentContext
    agentThreadId <-
        launch (\unmask -> run unmask agentContext)
            `onException` destroyAgentContext agentContext
    pure $ AgentHandle{agentThreadId, agentContext, agentCloseState}

newAgentOn :: Int -> AgentConfig -> IO AgentHandle
newAgentOn capability = newAgentHandle (Async.asyncOnWithUnmask capability)

spawnAgent :: AgentConfig -> IO Agent
spawnAgent config = Single <$> newAgentHandle Async.asyncWithUnmask config

cleanupUnstartedMulti :: Ptr CurlMulti -> IO ()
cleanupUnstartedMulti multiPtr =
    [CU.block| void { (void)curl_multi_cleanup($(CURLM* multiPtr)); } |]

new :: Ptr CurlMulti -> IO AgentContext
new multiPtr = mask_ do
    msgQueue <- initMPSCQ 131072
    nextId <- Boxed.newIORef 0
    contextStatus <- newMVar AgentContextRunning
    uvLoopPtr <-
        [C.block| uv_loop_t* {
        uv_loop_t *loop = malloc(sizeof(*loop));
        if (loop == NULL) {
            return NULL;
        }
        if (uv_loop_init(loop) != 0) {
            free(loop);
            return NULL;
        }
        return loop;
    }|]
    when (uvLoopPtr == nullPtr) $ throwIO $ userError "hcurl: unable to initialize libuv loop"
    uvAsyncPtr <- withMPSCQ msgQueue \mpscqPtr ->
        [C.block|uv_async_t* {
            return init_async_check_messages(
                $(uv_loop_t* uvLoopPtr), $(mpsc_t* mpscqPtr),
                $(CURLM* multiPtr));
    }
    |]
    when (uvAsyncPtr == nullPtr) do
        [CU.block|void {
            uv_loop_close($(uv_loop_t* uvLoopPtr));
            free($(uv_loop_t* uvLoopPtr));
        }|]
        throwIO $ userError "hcurl: unable to initialize agent reactor"
    senderPtr <-
        [CU.exp| message_sender_t* {
            message_sender_acquire($(uv_async_t* uvAsyncPtr))
        } |]
    when (senderPtr == nullPtr) do
        destroyUnboundReactor msgQueue uvLoopPtr uvAsyncPtr
        throwIO $ userError "hcurl: unable to retain agent message sender"
    messageSender <-
        (MessageSender <$> newForeignPtr finalizerMessageSender senderPtr)
            `onException` do
                destroyUnboundReactor msgQueue uvLoopPtr uvAsyncPtr
                [CU.block| void {
                    message_sender_release($(message_sender_t* senderPtr));
                } |]
    bound <-
        [CU.exp| int {
            bind_uv_curl_multi(
                $(uv_loop_t* uvLoopPtr), $(CURLM* multiPtr),
                $(uv_async_t* uvAsyncPtr)->data) ? 1 : 0
        } |]
    unless (bound /= 0) do
        destroyUnboundReactor msgQueue uvLoopPtr uvAsyncPtr
        throwIO $ userError "hcurl: unable to bind curl multi to libuv"
    pure $ AgentContext{uvAsync = UVAsync uvAsyncPtr, uvLoop = UVLoop uvLoopPtr, multi = multiPtr, ..}

destroyUnboundReactor :: MPSCQ -> Ptr UVLoop -> Ptr UVAsync -> IO ()
destroyUnboundReactor queue uvLoopPtr uvAsyncPtr =
    withMPSCQ queue \_queuePtr ->
        [CU.block| void {
            agent_shutdown($(uv_loop_t* uvLoopPtr), $(uv_async_t* uvAsyncPtr), NULL);
        } |]

finalizerMessageSender :: FunPtr (Ptr MessageSender -> IO ())
finalizerMessageSender =
    [C.funPtr| void hcurl_message_sender_finalizer(message_sender_t *sender) {
        message_sender_release(sender);
    } |]

run :: (forall value. IO value -> IO value) -> AgentContext -> IO ()
run unmask ctx = mask_ do
    let UVLoop uvLoopPtr = ctx.uvLoop
    outcome <- try @SomeException . unmask $
        withMPSCQ ctx.msgQueue \_queuePtr ->
            withMessageSender ctx.messageSender \_senderPtr ->
                [C.block|void {
                    (void)uv_run($(uv_loop_t* uvLoopPtr), UV_RUN_DEFAULT);
                }|]
    modifyMVarMasked_ ctx.contextStatus \case
        AgentContextRunning -> pure AgentContextStopping
        status -> pure status
    shutdownOutcome <- try @SomeException $ destroyAgentContext ctx
    let finalOutcome = outcome >> shutdownOutcome
    modifyMVarMasked_ ctx.contextStatus $ const $ pure (AgentContextStopped finalOutcome)
    either throwIO pure finalOutcome

destroyAgentContext :: AgentContext -> IO ()
destroyAgentContext ctx = do
    let UVLoop uvLoopPtr = ctx.uvLoop
        UVAsync uvAsyncPtr = ctx.uvAsync
        multiPtr = ctx.multi
    withMPSCQ ctx.msgQueue \_queuePtr ->
        withMessageSender ctx.messageSender \_senderPtr ->
            [CU.block|void {
                agent_shutdown($(uv_loop_t* uvLoopPtr), $(uv_async_t* uvAsyncPtr), $(CURLM* multiPtr));
            }|]

data QueueFull = QueueFull deriving (Show, Exception)

data MessageAllocationFailed = MessageAllocationFailed deriving (Show, Exception)

sendMessage :: AgentContext -> OuterMessage -> IO ()
sendMessage ctx outerMessage =
    modifyMVarMasked_ ctx.contextStatus \status -> case status of
        AgentContextRunning -> do
            sendMessageUnchecked ctx outerMessage
            pure case outerMessage of
                StopAgent -> AgentContextStopping
                _ -> AgentContextRunning
        AgentContextStopping -> case outerMessage of
            Execute{} -> throwIO AgentClosed
            _ -> pure AgentContextStopping
        AgentContextStopped{} -> case outerMessage of
            Execute{} -> throwIO AgentClosed
            _ -> pure status

sendMessageUnchecked :: AgentContext -> OuterMessage -> IO ()
sendMessageUnchecked ctx outerMessage =
    case outerMessage of
        Execute transferId easy result metrics streams ->
            withEasyData result \resultPtr ->
                withCurlMetricsContext metrics \metricsPtr ->
                    withMaybeStream streams.downloadStream \downloadPtr ->
                        withMaybeStream streams.uploadStream \uploadPtr ->
                            enqueue transferId easy resultPtr metricsPtr downloadPtr uploadPtr InternalExecute
        CancelRequest transferId ->
            enqueue transferId nullPtr nullPtr nullPtr nullPtr nullPtr InternalCancelRequest
        ResumeRequest transferId ->
            enqueue transferId nullPtr nullPtr nullPtr nullPtr nullPtr InternalResumeRequest
        StopAgent ->
            enqueue (TransferId 0) nullPtr nullPtr nullPtr nullPtr nullPtr InternalStopAgent
  where
    withMaybeStream :: Maybe CurlStream -> (Ptr CurlStream -> IO a) -> IO a
    withMaybeStream Nothing action = action nullPtr
    withMaybeStream (Just stream) action = withCurlStream stream action

    enqueue (TransferId identifier) easyPtr resultPtr metricsPtr downloadPtr uploadPtr tag = do
        let tagC = fromIntegral (fromEnum tag) :: CInt
        status <- withMessageSender ctx.messageSender \senderPtr ->
            [CU.block|int {
                return (int)enqueue_outer_message(
                    $(message_sender_t* senderPtr),
                    (enum outer_message_types)$(int tagC),
                    (transfer_id_t)$(uint64_t identifier),
                    $(CURL* easyPtr),
                    $(hs_easy_data_t* resultPtr),
                    $(curl_metrics_context_t* metricsPtr),
                    $(hcurl_stream_t* downloadPtr),
                    $(hcurl_stream_t* uploadPtr));
            }|]
        case status of
            0 -> pure ()
            1 -> throwIO QueueFull
            2 -> throwIO MessageAllocationFailed
            _ -> throwIO AgentClosed

selectAgentHandle :: Agent -> IO AgentHandle
selectAgentHandle = \case
    Single agentHandle -> pure agentHandle
    Threaded handles roundRobin _closeState -> selectRunningThreaded handles roundRobin
    Managed _ -> throwIO $ userError "Managed agents require acquireLease"

selectRunningThreaded :: NonEmpty AgentHandle -> RoundRobin AgentHandle -> IO AgentHandle
selectRunningThreaded handles roundRobin = go $ NonEmpty.length handles
  where
    go 0 = throwIO AgentClosed
    go attempts = do
        candidate <- RoundRobin.select roundRobin
        running <- agentHandleRunning candidate
        if running then pure candidate else go (attempts - 1)

{- | Number of selectable workers. Closed single/threaded workers and draining
managed workers are not counted.
-}
agentWorkerCount :: Agent -> IO (Maybe Int)
agentWorkerCount = \case
    Single agentHandle -> Just . fromEnum <$> agentHandleRunning agentHandle
    Threaded handles _ _ -> Just . length <$> filterM agentHandleRunning (NonEmpty.toList handles)
    Managed managedAgent ->
        withMVar managedAgent.maState $ fmap (Just . length) . selectableWorkers

{- | Atomically pick a worker for a new transfer and register it as active.
Growth is decided here: when every selectable worker is already busy
enough, a new agent is started synchronously before the request is served.
The returned 'leaseDone' action must be run once when the transfer is
finished or abandoned. Selection only ever routes to non-draining workers.
-}
acquireLease :: Agent -> IO Lease
acquireLease = \case
    Single agentHandle -> directLease agentHandle
    Threaded handles roundRobin _closeState -> do
        agentHandle <- selectRunningThreaded handles roundRobin
        directLease agentHandle
    Managed managedAgent -> do
        (workerId, agentHandle) <- modifyMVarMasked managedAgent.maState $ \managedState -> do
            when managedState.msClosed $ throwIO AgentClosed
            now <- getMonotonicTimeNSec
            (worker, stateAfter) <- admissionPick managedState.msPolicy managedState.msConfig now managedState
            incrementWorker worker
            pure (stateAfter, (worker.mwId, worker.mwHandle))
        let release = do
                mask_ do
                    retiring <- modifyMVarMasked managedAgent.maState $ \managedState -> do
                        releaseWorker workerId managedState
                    retireManagedWorkers managedAgent retiring
        pure $ Lease agentHandle release
  where
    directLease agentHandle = do
        running <- agentHandleRunning agentHandle
        unless running $ throwIO AgentClosed
        pure $ Lease agentHandle (pure ())

{- | Ask an agent to stop and wait for its loop to finish. Only idle agents
(no active transfers, no outstanding leases) should ever be stopped.
-}
stopAgent :: AgentHandle -> IO ()
stopAgent AgentHandle{agentThreadId, agentContext, agentCloseState} =
    runOnce agentCloseState do
        requestStop
        Async.wait agentThreadId
  where
    requestStop =
        sendMessage agentContext StopAgent
            `catches` [ Handler \QueueFull -> threadDelay 50 >> requestStop
                      , Handler \MessageAllocationFailed -> threadDelay 50 >> requestStop
                      , Handler \AgentClosed -> pure ()
                      ]

{- | Close every reactor owned by an agent. Active transfers are completed
with @AbortedByCallback@; concurrent calls wait for the same close result.
-}
closeAgent :: Agent -> IO ()
closeAgent = \case
    Single agentHandle -> stopAgent agentHandle
    Threaded handles _roundRobin closeState ->
        runOnce closeState $ releaseAllAgents $ NonEmpty.toList handles
    Managed managedAgent ->
        runOnce managedAgent.maCloseState $ closeManagedAgent managedAgent

withAgent :: AgentConfig -> (Agent -> IO a) -> IO a
withAgent config = bracket (spawnAgent config) closeAgent

withThreadedAgent :: Int -> AgentConfig -> (Agent -> IO a) -> IO a
withThreadedAgent count config = bracket (spawnThreadedAgent count config) closeAgent

withManagedAgent :: ManagedPolicy -> AgentConfig -> (Agent -> IO a) -> IO a
withManagedAgent policy config = bracket (spawnManagedAgent policy config) closeAgent

releaseAllAgents :: [AgentHandle] -> IO ()
releaseAllAgents handles = do
    outcomes <- Async.mapConcurrently (try @SomeException . stopAgent) handles
    case lefts outcomes of
        exception : _ -> throwIO exception
        [] -> pure ()

retireAgent :: AgentHandle -> IO ()
retireAgent agentHandle = ignoreSynchronousException $ stopAgent agentHandle

-- Stop first, remove from the registry second. If this thread is interrupted,
-- the worker remains discoverable and another controller tick or pool close
-- retries the idempotent stop.
retireManagedWorkers :: ManagedAgent -> [ManagedWorker] -> IO ()
retireManagedWorkers managedAgent workers = do
    mapM_ (retireAgent . mwHandle) workers
    let retiredIds = map mwId workers
    modifyMVarMasked_ managedAgent.maState $ \state ->
        pure
            state
                { msWorkers =
                    filter (\worker -> mwId worker `notElem` retiredIds) state.msWorkers
                }

closeManagedAgent :: ManagedAgent -> IO ()
closeManagedAgent managedAgent = do
    handles <- modifyMVarMasked managedAgent.maState $ \state -> do
        forM_ state.msWorkers markWorkerQuiescing
        pure (state{msClosed = True}, map mwHandle state.msWorkers)
    controller <- readMVar managedAgent.maController
    controllerOutcome <- try @SomeException $ Async.cancel controller
    workersOutcome <- try @SomeException $ releaseAllAgents handles
    modifyMVarMasked_ managedAgent.maState $ \state -> do
        let closedState = state{msWorkers = [], msLoad = 0, msShrinkStreak = 0}
        pure closedState
    either throwIO pure (controllerOutcome >> workersOutcome)

{- | Pick the least loaded selectable worker. When every selectable worker
already carries at least 'mpGrowLoad' active transfers and the spawn
cooldown has elapsed, a new agent is started synchronously and the new
request is routed to it. This is the fast growth path: a sudden demand
spike gets an extra worker at the moment of the request, not on the next
controller tick.
-}
admissionPick :: ManagedPolicy -> AgentConfig -> Word64 -> ManagedState -> IO (ManagedWorker, ManagedState)
admissionPick policy config now managedState = do
    selectable <- selectableWorkers managedState
    case selectable of
        [] -> spawnManagedWorker policy config now managedState
        _ -> do
            activeCounts <- forM selectable \worker -> (worker,) <$> readIORefU worker.mwActive
            let (least, leastActive) = minimumBy (comparing snd) activeCounts
                workerCount = length selectable
                spawnCooldownNs = microsToNanos policy.mpSpawnCooldownMicros
            if leastActive >= policy.mpGrowLoad
                && workerCount < policy.mpMaxAgents
                && now - managedState.msLastSpawn >= spawnCooldownNs
                then do
                    spawnManagedWorker policy config now managedState
                else pure (least, managedState)

spawnManagedWorker :: ManagedPolicy -> AgentConfig -> Word64 -> ManagedState -> IO (ManagedWorker, ManagedState)
spawnManagedWorker policy config now state = do
    let index = state.msNextId
    when (index == maxBound) $ throwIO $ userError "hcurl: managed worker ID space exhausted"
    agentHandle <- newAgentOn (index `mod` max 1 policy.mpMaxAgents) config
    worker <- newManagedWorker index agentHandle `onException` stopAgent agentHandle
    let grown =
            state
                { msWorkers = state.msWorkers <> [worker]
                , msShrinkStreak = 0
                , msLastSpawn = now
                , msNextId = index + 1
                }
    pure (worker, grown)

incrementWorker :: ManagedWorker -> IO ()
incrementWorker worker =
    modifyIORefU worker.mwActive (+ 1)

{- | Release one lease. If the worker is draining and this was its last
in-flight transfer, hand it back for stopping while retaining registry
ownership until that stop completes.
-}
releaseWorker :: Int -> ManagedState -> IO (ManagedState, [ManagedWorker])
releaseWorker workerId managedState = do
    let workers = managedState.msWorkers
    released <- forM workers $ \worker ->
        if worker.mwId == workerId
            then do
                active <- readIORefU worker.mwActive
                let newActive = max 0 (active - 1)
                writeIORefU worker.mwActive newActive
                quiescing <- workerIsQuiescing worker
                pure $ Just (worker, newActive, quiescing)
            else pure Nothing
    case catMaybes released of
        [(worker, 0, True)] -> pure (managedState, [worker])
        _ -> pure (managedState, [])

newTransferId :: AgentContext -> IO TransferId
newTransferId context = do
    identifier <- Boxed.atomicModifyIORef' context.nextId \current ->
        if current == maxBound
            then (current, Nothing)
            else
                let next = current + 1
                 in (next, Just next)
    maybe (throwIO TransferIdExhausted) (pure . TransferId) identifier

resumeRequest :: AgentContext -> TransferId -> IO ()
resumeRequest ctx transferId = sendMessage ctx $ ResumeRequest transferId
