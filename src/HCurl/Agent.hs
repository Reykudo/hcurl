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

module HCurl.Agent where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Concurrent.Async (Async)
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (filterM, forM_, forever, unless)
import Data.IORef
import Data.List (minimumBy)
import Data.Maybe (catMaybes)
import Data.Ord (comparing)
import Data.RoundRobin (RoundRobin, newRoundRobin)
import Data.RoundRobin qualified as RoundRobin
import Data.Traversable
import Data.Word (Word64)
import Foreign.Marshal (toBool)
import Foreign.Ptr
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Generics
import HCurl.Internal.MPSC
import HCurl.Internal.Multi
import HCurl.Internal.Raw
import HCurl.Internal.Raw.MPSC
import HCurl.Internal.Raw.UV
import HCurl.Request
import HCurl.Types
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU
import PyF

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
    }
    deriving (Generic)

data AgentHandle = AgentHandle
    { agentThreadId :: !(Async ())
    , agentContext :: !AgentContext
    }
    deriving (Generic)

data Agent
    = Single AgentHandle
    | Threaded (RoundRobin AgentHandle)
    | Managed ManagedAgent

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

{- | Defaults: one agent per capability, quarter-second sampling, grow at
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

data ManagedWorker = ManagedWorker
    { mwId :: !Int
    , mwHandle :: !AgentHandle
    , mwActive :: !(IORef Int)
    , mwUtilization :: !(IORef Double)
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
    }

-- | A snapshot of the managed pool handed to the metrics hook.
data ManagedMetrics = ManagedMetrics
    { mmRunningAgents :: !Int
    , mmDemand :: !Double
    }
    deriving (Show, Eq)

{- | Callback invoked by the pool controller whenever it samples the pool.
The hook is called outside the pool lock and exceptions it throws are
swallowed so a misbehaving metrics exporter cannot kill the controller.
Wire this up to whatever metrics backend you use (e.g. Prometheus gauges).
-}
type MetricsHook = ManagedMetrics -> IO ()

data ManagedAgent = ManagedAgent
    { maState :: !(MVar ManagedState)
    , maHook :: !(IORef MetricsHook)
    }

data Lease = Lease
    { leaseAgentHandle :: !AgentHandle
    , leaseDone :: !(IO ())
    }

spawnThreadedAgent :: Int -> AgentConfig -> IO Agent
spawnThreadedAgent numThreads config = do
    realCapabilities <- getNumCapabilities
    let agentCount = max 1 (min realCapabilities numThreads)
    handles <- for [0 .. agentCount - 1] $ \cap -> newAgentOn cap config
    rr <- newRoundRobin handles
    pure $ Threaded rr

{- | Start a demand-driven pool of agents.

The pool starts with @mpMinAgents@ workers. Growth happens synchronously
when requests are admitted; shrinking is handled by a controller thread
from the period-averaged demand (see 'ManagedPolicy'). Draining never
interrupts in-flight transfers: a draining worker stops receiving new
requests and its loop is torn down only once it has no active transfers
left.
-}
spawnManagedAgent :: ManagedPolicy -> AgentConfig -> IO Agent
spawnManagedAgent rawPolicy config = do
    realCapabilities <- getNumCapabilities
    let policy = clampPolicy realCapabilities rawPolicy
        startCount = policy.mpMinAgents
    workers <- for [0 .. startCount - 1] $ \index -> do
        agentHandle <- newAgentOn index config
        newManagedWorker index agentHandle
    state <- newMVar $ ManagedState workers 0 0 0 policy config startCount
    hook <- newIORef $ \_ -> pure ()
    _controller <- Async.async do
        controllerLoop (ManagedAgent state hook) policy
            `catch` \(ex :: SomeException) -> print [fmt|scaling controller died with exception {show ex}|]
    let agent = Managed $ ManagedAgent state hook
    runHook hook (ManagedMetrics startCount 0)
    pure agent

controllerLoop :: ManagedAgent -> ManagedPolicy -> IO ()
controllerLoop managedAgent policy = forever do
    threadDelay policy.mpTickMicros
    now <- getMonotonicTimeNSec
    stops <- modifyMVar managedAgent.maState $ \managedState -> controllerTick policy now managedState
    stateAfter <- readMVar managedAgent.maState
    runHook managedAgent.maHook (metricsOf stateAfter)
    mapM_ (Async.async . stopAgent) stops

{- | Set (or replace) the metrics hook of a 'Managed' agent. The hook is
invoked with a fresh snapshot immediately, and then after every controller
tick. 'Single' and 'Threaded' agents have no demand controller and reject
registration.
-}
registerManagedMetrics :: Agent -> MetricsHook -> IO ()
registerManagedMetrics agent hook = case agent of
    Managed managedAgent -> do
        writeIORef managedAgent.maHook hook
        snapshot <- metricsOf <$> readMVar managedAgent.maState
        runHook managedAgent.maHook snapshot
    _ -> throwIO $ userError "metrics hook requires a Managed agent"

-- | Current snapshot: number of running agent loops and the smoothed demand.
sampleManagedMetrics :: Agent -> IO (Maybe ManagedMetrics)
sampleManagedMetrics = \case
    Single _ -> pure Nothing
    Threaded _ -> pure Nothing
    Managed managedAgent -> Just . metricsOf <$> readMVar managedAgent.maState

metricsOf :: ManagedState -> ManagedMetrics
metricsOf managedState =
    ManagedMetrics
        { mmRunningAgents = length managedState.msWorkers
        , mmDemand = managedState.msLoad
        }

runHook :: IORef MetricsHook -> ManagedMetrics -> IO ()
runHook hookRef snapshot = do
    hook <- readIORef hookRef
    _ <- try @SomeException (hook snapshot)
    pure ()

newManagedWorker :: Int -> AgentHandle -> IO ManagedWorker
newManagedWorker index agentHandle =
    ManagedWorker index agentHandle <$> newIORef 0 <*> newIORef 0 <*> newIORef False

clampPolicy :: Int -> ManagedPolicy -> ManagedPolicy
clampPolicy realCapabilities policy =
    ManagedPolicy
        { mpMinAgents = max 1 policy.mpMinAgents
        , mpMaxAgents = max (max 1 policy.mpMinAgents) (min policy.mpMaxAgents realCapabilities)
        , mpTickMicros = max 10_000 policy.mpTickMicros
        , mpSustainTicks = max 1 policy.mpSustainTicks
        , mpEwmaAlpha = max 0.01 (min 1 policy.mpEwmaAlpha)
        , mpGrowLoad = max 1 policy.mpGrowLoad
        , mpShrinkLoad = max 0 policy.mpShrinkLoad
        , mpSpawnCooldownMicros = max 0 policy.mpSpawnCooldownMicros
        , mpKillCooldownMicros = max 0 policy.mpKillCooldownMicros
        }

selectableWorkers :: ManagedState -> IO [ManagedWorker]
selectableWorkers managedState =
    filterM (fmap not . readIORef . mwQuiescing) managedState.msWorkers

{- | One controller tick: sample demand, update the EWMA ("recommendation")
and decide whether to drain. Growth is handled synchronously at request
admission, never here. A drain is only started once the smoothed
per-worker demand has been at or below 'mpShrinkLoad' for
'mpSustainTicks' consecutive ticks and no spawn happened within the kill
cooldown, so fresh workers are not killed right after being started.
Returns handles of agents whose loops should now be stopped.
-}
controllerTick :: ManagedPolicy -> Word64 -> ManagedState -> IO (ManagedState, [AgentHandle])
controllerTick policy now managedState = do
    actives <- forM managedState.msWorkers $ \worker -> do
        active <- readIORef worker.mwActive
        quiescing <- readIORef worker.mwQuiescing
        pure (worker, active, quiescing)
    let alpha = policy.mpEwmaAlpha
        selectable = [(worker, active) | (worker, active, quiescing) <- actives, not quiescing]
        workerCount = length selectable
        sampleLoad = fromIntegral (sum (map snd selectable)) :: Double
        ewmaLoad = managedState.msLoad + alpha * (sampleLoad - managedState.msLoad)
    forM_ actives $ \(worker, active, _) -> do
        current <- readIORef worker.mwUtilization
        let busy = if active > 0 then 1 else 0 :: Double
        writeIORef worker.mwUtilization $ current + alpha * (busy - current)
    let withLoad = managedState{msLoad = ewmaLoad}
        perWorker = if workerCount == 0 then 0 else ewmaLoad / fromIntegral workerCount
        killCooldownNs = fromIntegral policy.mpKillCooldownMicros * 1000
        canKill = now - managedState.msLastSpawn >= killCooldownNs
        readyToShrink = workerCount > policy.mpMinAgents && canKill
    decided <-
        if readyToShrink && perWorker <= policy.mpShrinkLoad
            then shrinkStep withLoad
            else pure withLoad{msShrinkStreak = 0}
    drainFinished decided
  where
    shrinkStep :: ManagedState -> IO ManagedState
    shrinkStep state = do
        let streak = state.msShrinkStreak + 1
        if streak >= policy.mpSustainTicks
            then do
                selectable <- selectableWorkers state
                ranked <- forM selectable $ \worker -> do
                    active <- readIORef worker.mwActive
                    utilization <- readIORef worker.mwUtilization
                    pure (worker, active, utilization)
                case ranked of
                    [] -> pure state
                    _ -> do
                        let (victim, _, _) = minimumBy (comparing (\(_, active, utilization) -> (active, utilization))) ranked
                        writeIORef victim.mwQuiescing True
                        pure state{msShrinkStreak = 0}
            else pure state{msShrinkStreak = streak}

-- | Remove draining workers whose transfers have all finished.
drainFinished :: ManagedState -> IO (ManagedState, [AgentHandle])
drainFinished managedState = do
    drained <- forM managedState.msWorkers $ \worker -> do
        active <- readIORef worker.mwActive
        quiescing <- readIORef worker.mwQuiescing
        pure (worker, active, quiescing)
    let finished = [worker | (worker, active, quiescing) <- drained, quiescing, active == 0]
    if null finished
        then pure (managedState, [])
        else do
            let finishedIds = map mwId finished
                remaining = filter (\worker -> mwId worker `notElem` finishedIds) managedState.msWorkers
            pure (managedState{msWorkers = remaining}, map mwHandle finished)

newAgentOn :: Int -> AgentConfig -> IO AgentHandle
newAgentOn capability config = do
    multi <- initCurlMulti config
    agentContext <- new multi
    agentThreadId <- Async.asyncOn capability do
        run agentContext `catch` \(ex :: SomeException) -> print [fmt|agent died with exception {show ex}|]
    pure $ AgentHandle{agentThreadId, agentContext}

spawnAgent :: AgentConfig -> IO Agent
spawnAgent config = do
    multi <- initCurlMulti config

    agentContext <- new multi

    agentThreadId <- Async.async $ do
        run agentContext `catch` \(ex :: SomeException) -> print [fmt|agent died with exception {show ex}|]
    pure . Single $ AgentHandle{agentThreadId, agentContext}

new :: Ptr CurlMulti -> IO AgentContext
new multiPtr = do
    msgQueue <- initMPSCQ 100000
    uvLoopPtr <-
        [C.block| uv_loop_t* {
        uv_loop_t *loop = malloc(sizeof(uv_loop_t));
        uv_loop_init(loop);
        return loop;
    }|]
    uvAsyncPtr <- withMPSCQ msgQueue \mpscqPtr ->
        [C.block|uv_async_t* {
        bind_uv_curl_multi($(uv_loop_t* uvLoopPtr), $(CURLM* multiPtr));
        uv_async_t* uv_async = init_async_check_messages($(uv_loop_t* uvLoopPtr), $(mpsc_t* mpscqPtr), $(CURLM* multiPtr));
        return uv_async;
    }
    |]
    pure $ AgentContext{uvAsync = UVAsync uvAsyncPtr, uvLoop = UVLoop uvLoopPtr, multi = multiPtr, ..}

run :: AgentContext -> IO ()
run ctx = do
    let UVLoop uvLoopPtr = ctx.uvLoop
    [C.block|void {
        uv_run($(uv_loop_t* uvLoopPtr), UV_RUN_DEFAULT);
    }|]
    -- uv_run returns after a STOP_AGENT message; tear the loop down. The
    -- agent was already removed from its pool, so no more messages can
    -- arrive. The message queue itself is released by its finalizer.
    let UVAsync uvAsyncPtr = ctx.uvAsync
        multiPtr = ctx.multi
    [CU.block|void {
        agent_shutdown($(uv_loop_t* uvLoopPtr), $(uv_async_t* uvAsyncPtr), $(CURLM* multiPtr));
    }|]

data QueueFull = QueueFull deriving (Show, Exception)

sendMessage :: AgentContext -> OuterMessage -> IO ()
sendMessage ctx outerMessage = do
    InternalOuterMessage msgPtr <- toInnerOuterMessage outerMessage
    let UVAsync asyncPtr = ctx.uvAsync
    isEnqueued <- withMPSCQ ctx.msgQueue \mpscPtr ->
        [CU.block|bool {
        bool isEnqueued = mpscq_enqueue($(mpsc_t* mpscPtr), $(outer_message_t* msgPtr));
        if (isEnqueued) {
            uv_async_send($(uv_async_t* asyncPtr));
        }
        return isEnqueued;
    }|]
    unless (toBool isEnqueued) do
        [CU.block|void { destroy_outer_message($(outer_message_t* msgPtr)); }|]
        throwIO QueueFull

selectAgentHandle :: Agent -> IO AgentHandle
selectAgentHandle = \case
    Single agentHandle -> pure agentHandle
    Threaded handles -> RoundRobin.select handles
    Managed _ -> throwIO $ userError "Managed agents require acquireLease"

{- | Number of live workers, where observable. 'Single' is always one;
'Threaded' keeps its worker count inside the round-robin table and reports
'Nothing'; 'Managed' reports the current pool size.
-}
agentWorkerCount :: Agent -> IO (Maybe Int)
agentWorkerCount = \case
    Single _ -> pure $ Just 1
    Threaded _ -> pure Nothing
    Managed managedAgent -> do
        workers <- msWorkers <$> readMVar managedAgent.maState
        selectable <- filterM (fmap not . readIORef . mwQuiescing) workers
        pure $ Just (length selectable)

{- | Atomically pick a worker for a new transfer and register it as active.
Growth is decided here: when every selectable worker is already busy
enough, a new agent is started synchronously before the request is served.
The returned 'leaseDone' action must be run once when the transfer is
finished or abandoned. Selection only ever routes to non-draining workers.
-}
acquireLease :: Agent -> IO Lease
acquireLease = \case
    Single agentHandle -> pure $ Lease agentHandle (pure ())
    Threaded handles -> do
        agentHandle <- RoundRobin.select handles
        pure $ Lease agentHandle (pure ())
    Managed managedAgent -> do
        (workerId, agentHandle) <- modifyMVar managedAgent.maState $ \managedState -> do
            now <- getMonotonicTimeNSec
            (worker, stateAfter) <- admissionPick managedState.msPolicy managedState.msConfig now managedState
            incrementWorker worker
            pure (stateAfter, (worker.mwId, worker.mwHandle))
        let release = do
                stopped <- modifyMVar managedAgent.maState $ \managedState -> do
                    releaseWorker workerId managedState
                mapM_ (Async.async . stopAgent) stopped
        pure $ Lease agentHandle release

{- | Ask an agent to stop and wait for its loop to finish. Only idle agents
(no active transfers, no outstanding leases) should ever be stopped.
-}
stopAgent :: AgentHandle -> IO ()
stopAgent AgentHandle{agentThreadId, agentContext} = do
    sendMessage agentContext StopAgent
    Async.wait agentThreadId

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
        [] -> throwIO $ userError "managed pool has no selectable workers"
        _ -> do
            activeCounts <- forM selectable \worker -> (worker,) <$> readIORef worker.mwActive
            let (least, leastActive) = minimumBy (comparing snd) activeCounts
                workerCount = length selectable
                spawnCooldownNs = fromIntegral policy.mpSpawnCooldownMicros * 1000
            if leastActive >= policy.mpGrowLoad
                && workerCount < policy.mpMaxAgents
                && now - managedState.msLastSpawn >= spawnCooldownNs
                then do
                    let index = managedState.msNextId
                    agentHandle <- newAgentOn (index `mod` max 1 policy.mpMaxAgents) config
                    worker <- newManagedWorker index agentHandle
                    let grown =
                            managedState
                                { msWorkers = managedState.msWorkers <> [worker]
                                , msShrinkStreak = 0
                                , msLastSpawn = now
                                , msNextId = index + 1
                                }
                    pure (worker, grown)
                else pure (least, managedState)

incrementWorker :: ManagedWorker -> IO ()
incrementWorker worker =
    atomicModifyIORef' worker.mwActive \active -> (active + 1, ())

{- | Release one lease. If the worker is draining and this was its last
in-flight transfer, remove it and hand back its handle for stopping.
-}
releaseWorker :: Int -> ManagedState -> IO (ManagedState, [AgentHandle])
releaseWorker workerId managedState = do
    let workers = managedState.msWorkers
    released <- forM workers $ \worker ->
        if worker.mwId == workerId
            then do
                newActive <- atomicModifyIORef' worker.mwActive \active ->
                    let active' = max 0 (active - 1)
                     in (active', active')
                quiescing <- readIORef worker.mwQuiescing
                pure $ Just (worker, newActive, quiescing)
            else pure Nothing
    case catMaybes released of
        [(worker, 0, True)] -> do
            let remaining = filter (\candidate -> mwId candidate /= workerId) workers
            pure (managedState{msWorkers = remaining}, [worker.mwHandle])
        _ -> pure (managedState, [])

resumeRequest :: AgentContext -> CurlEasy -> IO ()
resumeRequest ctx (CurlEasy easyPtr) = sendMessage ctx $ ResumeRequest easyPtr

cancelRequest :: AgentContext -> RequestHandler response -> IO ()
cancelRequest ctx reqHandler = do
    let CurlEasy easyPtr = reqHandler.easy
    waker <- newEmptyMVar
    sendMessage ctx $ CancelRequest easyPtr waker
    readMVar waker
