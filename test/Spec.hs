module Main (main) where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Concurrent.Async (Async, async, cancel, forConcurrently, wait, waitCatch)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, bracket, finally, throwIO, try)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import HCurl.Agent (
    Agent,
    ManagedMetrics (..),
    ManagedPolicy (..),
    agentWorkerCount,
    registerManagedMetrics,
    spawnManagedAgent,
    spawnThreadedAgent,
 )
import HCurl.Internal.Raw (CurlCode (..))
import HCurl.Request (LowSpeedLimit (..), Request (..), RequestHeader (NoHeaders))
import HCurl.Response (HttpParts (..), Response (..), StreamingResponse (..))
import HCurl.Simple (httpLBS, initCurl)
import HCurl.Streaming (
    BodyReader,
    InvalidStreamBufferSize (..),
    StreamConfig (..),
    closeBody,
    httpStreaming,
    httpStreamingWith,
    readBody,
    withHttpStreaming,
 )
import HCurl.Types (AgentConfig (..), Body (Empty), HTTPMethod (Get), defaultConfig)
import Network.Socket
import Network.Socket.ByteString qualified as Socket
import System.IO (hPutStrLn, stderr)
import System.Timeout qualified as Timeout

main :: IO ()
main = withSocketsDo do
    initCurl
    checkDefaultConfig
    agent <- spawnThreadedAgent 4 defaultConfig
    runTest "threaded: concurrent buffered requests complete with correct bodies" $ testConcurrentBuffered agent
    runTest "threaded: failing transfers do not poison the agent" $ testFailureIsolation agent
    runTest "threaded: streaming backpressure pauses and resumes libcurl" $ testBackpressure agent
    runTest "threaded: early close cancels the transfer and the agent stays usable" $ testEarlyCancel agent
    runTest "threaded: truncated response reports a late curl error" $ testLateFailure agent
    managedAgent <- spawnManagedAgent (fastPolicy 2) defaultConfig
    runTest "managed: pool scales by measured demand and shrinks under sustained light traffic" $ testManagedDemandScaling managedAgent
    streamingAgent <- spawnManagedAgent (fastPolicy 2) defaultConfig
    runTest "managed: streaming leases drain and the pool shrinks under traffic" $ testManagedStreamingScaling streamingAgent
    metricsAgent <- spawnManagedAgent (fastPolicy 2) defaultConfig
    runTest "managed: metrics hook reports running agents and demand" $ testManagedMetricsHook metricsAgent
    runTest "streaming: abandoned reader becomes a deterministic error after scope exit" $ testAbandonedScopeExit agent
    runTest "streaming: scoped API closes the body on early return" $ testScopedEarlyReturn agent
    runTest "streaming: outer handler scope releases several abandoned requests" $ testHandlerScope agent
    runTest "streaming: blocked reader is woken when the stream is released" $ testBlockedReaderWoken agent
    runTest "streaming: zero buffered chunks is rejected up front" $ testZeroBufferConfig agent
    managedAbandonAgent <- spawnManagedAgent (fastPolicy 2) defaultConfig
    runTest "managed: abandoned streams release their leases at scope exit" $ testManagedAbandonedLeases managedAbandonAgent

fastPolicy :: Int -> ManagedPolicy
fastPolicy maxAgents =
    ManagedPolicy
        { mpMinAgents = 1
        , mpMaxAgents = maxAgents
        , mpTickMicros = 40_000
        , mpSustainTicks = 3
        , mpEwmaAlpha = 0.5
        , mpGrowLoad = 2
        , mpShrinkLoad = 0.4
        , mpSpawnCooldownMicros = 0
        , mpKillCooldownMicros = 150_000
        }

checkDefaultConfig :: IO ()
checkDefaultConfig =
    unless (defaultConfig == AgentConfig{maxConnection = 0, maxConnectionPerHost = 0, connectionCacheSize = 0}) $
        fail "HCurl default configuration is invalid"

runTest :: String -> IO () -> IO ()
runTest name action = do
    hPutStrLn stderr $ "[ RUN      ] " <> name
    outcome <- try @SomeException $ within 20_000_000 action
    case outcome of
        Left exception -> do
            hPutStrLn stderr $ "[  FAILED  ] " <> name
            throwIO exception
        Right () -> hPutStrLn stderr $ "[       OK ] " <> name

within :: Int -> IO a -> IO a
within microseconds action = do
    result <- Timeout.timeout microseconds action
    maybe (throwIO $ userError "test timed out") pure result

assertEqual :: (Eq a, Show a) => String -> a -> a -> IO ()
assertEqual label expected actual =
    unless (expected == actual) . throwIO . userError $
        label <> ": expected " <> show expected <> ", got " <> show actual

requestFor :: ByteString -> Request
requestFor url =
    Request
        { host = url
        , timeoutMS = 10_000
        , connectionTimeoutMS = 1_000
        , lowSpeedLimit = LowSpeedLimit{timeout = 0, lowSpeed = 0}
        , body = Empty
        , method = Get
        , headers = NoHeaders
        , extraOptions = []
        }

{- | Run many concurrent buffered requests through the round-robin agent.
Round-robin selection, per-agent execution, waker delivery and result
attribution all have to survive genuine parallelism.
-}
testConcurrentBuffered :: Agent -> IO ()
testConcurrentBuffered agent = do
    let payloads = [BS.replicate (64 * 1024) (fromIntegral (97 + i `mod` 26)) | i <- [1 .. 16]]
    results <- forConcurrently payloads \body ->
        withServer (sendFixedResponse "200 OK" [("X-HCurl-Thr", "multi")] body) \url ->
            runResourceT $ httpLBS agent (requestFor url)
    forM_ (zip payloads results) \(expected, result) ->
        case result of
            Left curlCode -> throwIO $ userError ("unexpected curl error: " <> show curlCode)
            Right Response{info = HttpParts{statusCode}, body} -> do
                assertEqual "threaded status" 200 statusCode
                assertEqual "threaded body" (BSL.fromStrict expected) body

{- | Half of the batch hits servers that close the connection mid-body.
Failures must surface as 'Left' curl codes while healthy transfers in the
same batch succeed, and the agent must accept requests afterwards.
-}
testFailureIsolation :: Agent -> IO ()
testFailureIsolation agent = do
    let healthyCount = 8
        brokenCount = 8
    healthyResults <- forConcurrently [1 .. healthyCount] \i -> do
        let body = BSC.pack $ "healthy-" <> show i
        withServer (sendFixedResponse "200 OK" [] body) \url ->
            runResourceT $ httpLBS agent (requestFor url)
    forM_ healthyResults \result ->
        case result of
            Left curlCode -> throwIO $ userError ("healthy request failed: " <> show curlCode)
            Right Response{info = HttpParts{statusCode}} ->
                assertEqual "healthy status" 200 statusCode
    brokenResults <- forConcurrently [1 .. brokenCount] \_ ->
        withServer shortResponder \url ->
            runResourceT $ httpLBS agent (requestFor url)
    forM_ brokenResults \result ->
        case result of
            Left _ -> pure ()
            Right _ -> throwIO $ userError "broken request unexpectedly succeeded"
    withServer (sendFixedResponse "200 OK" [] "after-failure") \url ->
        runResourceT (httpLBS agent (requestFor url)) >>= \case
            Left curlCode -> throwIO $ userError ("request after failures failed: " <> show curlCode)
            Right Response{body} -> assertEqual "body after failures" "after-failure" body

{- | A bounded one-chunk queue forces pause/resume handshakes while the body
is drained from a Haskell reader on the threaded agent.
-}
testBackpressure :: Agent -> IO ()
testBackpressure agent = do
    let payload = BS.replicate (1024 * 1024) 97
    withServer (sendFixedResponse "200 OK" [] payload) \url ->
        runResourceT do
            result <- httpStreamingWith StreamConfig{bufferedChunks = 1} agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("unexpected curl error: " <> show curlCode)
                Right StreamingResponse{body = reader, completion} -> liftIO do
                    threadDelay 100_000
                    (actual, streamResult) <- drainBody reader
                    assertEqual "body after pause/resume" payload actual
                    assertEqual "stream completion" (Right ()) streamResult
                    void completion

{- | Closing the body early must cancel the transfer (not hang), and a later
request on the same threaded agent must still succeed.
-}
testEarlyCancel :: Agent -> IO ()
testEarlyCancel agent =
    withServer sendForever \url ->
        runResourceT do
            result <- httpStreaming agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("response did not start: " <> show curlCode)
                Right StreamingResponse{info = HttpParts{statusCode}, body = reader, completion} -> liftIO do
                    assertEqual "status" 200 statusCode
                    first <- readBody reader
                    assertEqual "first chunk" (Right $ Just "first") first
                    closeBody reader
                    completion >>= assertEqual "completion after close" (Left AbortedByCallback)
                    afterClose <- readBody reader
                    assertEqual "read after close" (Left AbortedByCallback) afterClose
            `finally` withServer (sendFixedResponse "200 OK" [] "still-alive") \aliveUrl ->
                runResourceT (httpLBS agent (requestFor aliveUrl)) >>= \case
                    Left curlCode -> throwIO $ userError ("request after cancel failed: " <> show curlCode)
                    Right Response{body} -> assertEqual "body after cancel" "still-alive" body

testLateFailure :: Agent -> IO ()
testLateFailure agent =
    withServer shortResponder \url ->
        runResourceT do
            result <- httpStreaming agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("response did not start: " <> show curlCode)
                Right StreamingResponse{body = reader, completion} -> liftIO do
                    (actual, streamResult) <- drainBody reader
                    assertEqual "partial body" "short" actual
                    assertEqual "late stream error" (Left PartialFile) streamResult
                    completion >>= assertEqual "late completion error" (Left PartialFile)

{- | Growth must happen synchronously at request admission: two transfers
held open keep the single worker busy, and the very next request gets a
fresh worker. Shrinking must then drain back to the minimum while requests
keep arriving all the time - i.e. it must not depend on a fully idle
worker and must not race the admission spawn.
-}
testManagedDemandScaling :: Agent -> IO ()
testManagedDemandScaling agent = do
    realCapabilities <- getNumCapabilities
    when (realCapabilities >= 2) do
        start1 <- newEmptyMVar
        start2 <- newEmptyMVar
        releaseGate1 <- newEmptyMVar
        releaseGate2 <- newEmptyMVar
        let expected = BS.replicate 100 97 <> BS.replicate 100 98
        request1 <- async $ bufferedOne start1 releaseGate1
        request2 <- async $ bufferedOne start2 releaseGate2
        takeMVar start1
        takeMVar start2
        countWhileBusy <- agentWorkerCount agent
        assertEqual "pool stays at one while two transfers run" (Just 1) countWhileBusy
        probe <- withServer (sendFixedResponse "200 OK" [] "probe") \url ->
            runResourceT $ httpLBS agent (requestFor url)
        assertBufferedOk "admission probe" "probe" probe
        waitForWorkerCount "spawn happens synchronously at request admission" agent (Just 2)
        putMVar releaseGate1 ()
        putMVar releaseGate2 ()
        result1 <- wait request1
        result2 <- wait request2
        forM_ [result1, result2] $ \result -> assertBufferedOk "gated buffered" expected result
    traffic <- async $ lightTraffic agent 2_400_000
    when (realCapabilities >= 2) do
        waitForWorkerCount "pool shrinks under sustained light traffic" agent (Just 1)
    wait traffic
    countAfterTraffic <- agentWorkerCount agent
    assertEqual "pool at minimum after traffic" (Just 1) countAfterTraffic
  where
    bufferedOne started releaseGate =
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT $ httpLBS agent (requestFor url)

{- | Same shape through the streaming path: admission growth while two
streaming transfers are held open, then drain. Leases must be released
when the body is drained, otherwise the pool would stay at two under
traffic.
-}
testManagedStreamingScaling :: Agent -> IO ()
testManagedStreamingScaling agent = do
    realCapabilities <- getNumCapabilities
    when (realCapabilities >= 2) do
        start1 <- newEmptyMVar
        start2 <- newEmptyMVar
        releaseGate1 <- newEmptyMVar
        releaseGate2 <- newEmptyMVar
        let expected = BS.replicate 100 97 <> BS.replicate 100 98
        request1 <- async $ streamingOne start1 releaseGate1
        request2 <- async $ streamingOne start2 releaseGate2
        takeMVar start1
        takeMVar start2
        probe <- withServer (sendFixedResponse "200 OK" [] "probe") \url ->
            runResourceT $ httpLBS agent (requestFor url)
        assertBufferedOk "streaming admission probe" "probe" probe
        waitForWorkerCount "streaming spawn happens at request admission" agent (Just 2)
        putMVar releaseGate1 ()
        putMVar releaseGate2 ()
        body1 <- wait request1
        body2 <- wait request2
        assertEqual "gated streaming body 1" expected body1
        assertEqual "gated streaming body 2" expected body2
    traffic <- async $ lightTraffic agent 2_400_000
    when (realCapabilities >= 2) do
        waitForWorkerCount "streaming leases drain and pool shrinks under traffic" agent (Just 1)
    wait traffic
    countAfterTraffic <- agentWorkerCount agent
    assertEqual "streaming pool at minimum after traffic" (Just 1) countAfterTraffic
  where
    streamingOne started releaseGate =
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("stream did not start: " <> show curlCode)
                    Right StreamingResponse{body = reader, completion} -> liftIO do
                        (actual, streamResult) <- drainBody reader
                        assertEqual "gated streaming drain" (Right ()) streamResult
                        void completion
                        pure actual

{- | Continuous small requests with gaps in between: never a fully idle pool
for long, but clearly below the shrink threshold.
-}
lightTraffic :: Agent -> Int -> IO ()
lightTraffic agent totalMicros = go totalMicros
  where
    go remaining
        | remaining <= 0 = pure ()
        | otherwise = do
            body <- withServer (sendFixedResponse "200 OK" [] "lt") \url ->
                runResourceT $ httpLBS agent (requestFor url)
            assertBufferedOk "light traffic" "lt" body
            threadDelay 60_000
            go (remaining - 60_000)

{- | The metrics hook must be called with the number of running agents and
the smoothed demand: it has to go up while transfers are held open (the
admission probe grows the pool) and back down once they are gone and the
pool drains.
-}
testManagedMetricsHook :: Agent -> IO ()
testManagedMetricsHook agent = do
    realCapabilities <- getNumCapabilities
    recorder <- newIORef []
    registerManagedMetrics agent $ \snapshot ->
        atomicModifyIORef' recorder (\snapshots -> (snapshot : snapshots, ()))
    when (realCapabilities >= 2) do
        start1 <- newEmptyMVar
        start2 <- newEmptyMVar
        releaseGate1 <- newEmptyMVar
        releaseGate2 <- newEmptyMVar
        let expected = BS.replicate 100 97 <> BS.replicate 100 98
        request1 <- async $ bufferedOne start1 releaseGate1
        request2 <- async $ bufferedOne start2 releaseGate2
        takeMVar start1
        takeMVar start2
        probe <- withServer (sendFixedResponse "200 OK" [] "probe") \url ->
            runResourceT $ httpLBS agent (requestFor url)
        assertBufferedOk "metrics admission probe" "probe" probe
        waitForMetrics "hook reported growth to two running agents" recorder $
            any (\snapshot -> mmRunningAgents snapshot == 2)
        busyDemand <- readIORef recorder
        unless (any (\snapshot -> mmDemand snapshot >= 0.5) busyDemand) $
            throwIO $
                userError "hook never reported elevated demand while transfers were busy"
        putMVar releaseGate1 ()
        putMVar releaseGate2 ()
        result1 <- wait request1
        result2 <- wait request2
        forM_ [result1, result2] $ \result -> assertBufferedOk "gated metric buffered" expected result
        waitForMetrics "hook reported the pool draining back to one agent" recorder $ \case
            snapshot : _ -> mmRunningAgents snapshot == 1 && mmDemand snapshot <= 0.3
            [] -> False
  where
    bufferedOne started releaseGate =
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT $ httpLBS agent (requestFor url)

waitForMetrics :: String -> IORef [ManagedMetrics] -> ([ManagedMetrics] -> Bool) -> IO ()
waitForMetrics label recorder predicate = go 300
  where
    go 0 = do
        snapshots <- readIORef recorder
        throwIO $ userError $ label <> ": predicate never matched; recent snapshots " <> show (take 3 snapshots)
    go remaining = do
        snapshots <- readIORef recorder
        if predicate snapshots
            then pure ()
            else threadDelay 10_000 >> go (remaining - 1)

{- | A reader that is abandoned (never read, never closed) must become a
deterministic error once the scope it was created in exits.
-}
testAbandonedScopeExit :: Agent -> IO ()
testAbandonedScopeExit agent = do
    started <- newEmptyMVar
    releaseGate <- newEmptyMVar
    readerVar <- newEmptyMVar
    withServer (gatedResponder started releaseGate) \url -> do
        scopeThread <-
            async $ do
                reader <-
                    runResourceT do
                        result <- httpStreaming agent (requestFor url)
                        case result of
                            Left curlCode -> liftIO . throwIO $ userError ("stream did not start: " <> show curlCode)
                            Right StreamingResponse{body} -> pure body
                putMVar readerVar reader
        takeMVar started
        wait scopeThread
        reader <- takeMVar readerVar
        outcome <- readBody reader
        assertEqual "read after scope exit" (Left AbortedByCallback) outcome
        putMVar releaseGate ()
    withServer (sendFixedResponse "200 OK" [] "still-alive") \url ->
        runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after abandoned scope" "still-alive"

{- | The scoped API must close the body the moment the callback returns, even
when only the first chunk was read, and the agent stays usable.
-}
testScopedEarlyReturn :: Agent -> IO ()
testScopedEarlyReturn agent = do
    started <- newEmptyMVar
    releaseGate <- newEmptyMVar
    value <-
        withServer (gatedResponder started releaseGate) \url -> do
            result <-
                runResourceT $
                    withHttpStreaming agent (requestFor url) \response -> do
                        first <- liftIO $ readBody response.body
                        liftIO $ assertEqual "scoped first chunk" (Right $ Just (BS.replicate 100 97)) first
                        pure (42 :: Int)
            case result of
                Left curlCode -> throwIO $ userError ("scoped stream did not start: " <> show curlCode)
                Right scopedValue -> pure scopedValue
    assertEqual "scoped callback value" 42 value
    putMVar releaseGate ()
    withServer (sendFixedResponse "200 OK" [] "still-alive") \url ->
        runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after scoped early return" "still-alive"

{- | One outer handler scope runs several external requests and abandons a
streaming body; when the handler's runResourceT ends, everything must be
released and the escaped reader must fail deterministically.
-}
testHandlerScope :: Agent -> IO ()
testHandlerScope agent = do
    started <- newEmptyMVar
    releaseGate <- newEmptyMVar
    escaped <- newIORef Nothing
    withServer (sendFixedResponse "200 OK" [] "buffered-body") \bufferedUrl ->
        withServer (gatedResponder started releaseGate) \streamingUrl ->
            runResourceT do
                buffered <- httpLBS agent (requestFor bufferedUrl)
                liftIO $ assertBufferedOk "handler buffered" "buffered-body" buffered
                streamed <- httpStreaming agent (requestFor streamingUrl)
                case streamed of
                    Left curlCode -> liftIO . throwIO $ userError ("handler stream did not start: " <> show curlCode)
                    Right StreamingResponse{body = reader} -> do
                        liftIO $ writeIORef escaped (Just reader)
                        liftIO $ takeMVar started
                        liftIO $ putMVar releaseGate ()
                        pure ()
    escapedReader <- readIORef escaped
    reader <- maybe (throwIO $ userError "no escaped reader captured") pure escapedReader
    outcome <- readBody reader
    assertEqual "read after handler exit" (Left AbortedByCallback) outcome
    withServer (sendFixedResponse "200 OK" [] "after-handler") \url ->
        runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after handler scope" "after-handler"

{- | A zero-chunk bounded queue is a configuration error and must be rejected
before any transfer starts, not hang or crash inside the agent.
-}
testZeroBufferConfig :: Agent -> IO ()
testZeroBufferConfig agent =
    withServer (sendFixedResponse "200 OK" [] "x") \url -> do
        outcome <-
            try @InvalidStreamBufferSize $
                runResourceT $
                    httpStreamingWith StreamConfig{bufferedChunks = 0} agent (requestFor url)
        case outcome of
            Left InvalidStreamBufferSize -> pure ()
            Right _ -> throwIO $ userError "expected InvalidStreamBufferSize for zero buffered chunks"

{- | A thread parked in 'readBody' (no more data yet) must be woken with
'Left AbortedByCallback' when the stream is released from another thread,
instead of leaking a blocked thread forever.
-}
testBlockedReaderWoken :: Agent -> IO ()
testBlockedReaderWoken agent = do
    started <- newEmptyMVar
    releaseGate <- newEmptyMVar
    outcomeVar <- newEmptyMVar
    withServer (gatedResponder started releaseGate) \url ->
        runResourceT do
            result <- httpStreaming agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("stream did not start: " <> show curlCode)
                Right StreamingResponse{body = reader} -> do
                    _ <- liftIO $ async $ drainResult reader >>= putMVar outcomeVar
                    liftIO $ takeMVar started
                    liftIO $ threadDelay 100_000
                    liftIO $ closeBody reader
                    outcome <- liftIO $ within 5_000_000 $ takeMVar outcomeVar
                    liftIO $ assertEqual "blocked reader woken by close" (Left AbortedByCallback) outcome
    withServer (sendFixedResponse "200 OK" [] "still-alive") \url ->
        runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after blocked reader release" "still-alive"
  where
    drainResult :: BodyReader -> IO (Either CurlCode ())
    drainResult reader =
        readBody reader >>= \case
            Left curlCode -> pure $ Left curlCode
            Right Nothing -> pure $ Right ()
            Right (Just _) -> drainResult reader

{- | Abandoned streams whose scope exits must release their lease on the
managed pool: otherwise the pool could never drain back to the minimum,
because the abandoned worker would look busy forever.
-}
testManagedAbandonedLeases :: Agent -> IO ()
testManagedAbandonedLeases agent = do
    realCapabilities <- getNumCapabilities
    forM_ [1 .. 4] $ \_ -> do
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("abandoned stream did not start: " <> show curlCode)
                    Right _ -> liftIO $ takeMVar started
        putMVar releaseGate ()
    when (realCapabilities >= 2) do
        start1 <- newEmptyMVar
        start2 <- newEmptyMVar
        releaseGate1 <- newEmptyMVar
        releaseGate2 <- newEmptyMVar
        let expected = BS.replicate 100 97 <> BS.replicate 100 98
        request1 <- async $ bufferedOne start1 releaseGate1
        request2 <- async $ bufferedOne start2 releaseGate2
        takeMVar start1
        takeMVar start2
        probe <- withServer (sendFixedResponse "200 OK" [] "probe") \url ->
            runResourceT $ httpLBS agent (requestFor url)
        assertBufferedOk "lease probe" "probe" probe
        waitForWorkerCount "pool grows after abandoned streams" agent (Just 2)
        putMVar releaseGate1 ()
        putMVar releaseGate2 ()
        result1 <- wait request1
        result2 <- wait request2
        forM_ [result1, result2] $ \result -> assertBufferedOk "gated after abandoned" expected result
        traffic <- async $ lightTraffic agent 2_400_000
        waitForWorkerCount "pool drains despite abandoned streams" agent (Just 1)
        wait traffic
  where
    bufferedOne started releaseGate =
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT $ httpLBS agent (requestFor url)

assertBufferedOk :: String -> ByteString -> Either CurlCode (Response BSL.ByteString) -> IO ()
assertBufferedOk context expectedBody = \case
    Left curlCode -> throwIO $ userError (context <> ": unexpected curl error: " <> show curlCode)
    Right Response{info = HttpParts{statusCode}, body} -> do
        assertEqual (context <> " status") 200 statusCode
        assertEqual (context <> " body") (BSL.fromStrict expectedBody) body

waitForWorkerCount :: String -> Agent -> Maybe Int -> IO ()
waitForWorkerCount label agent expected = go 60
  where
    go 0 = do
        actual <- agentWorkerCount agent
        throwIO $ userError $ label <> ": expected worker count " <> show expected <> ", still " <> show actual
    go remaining = do
        actual <- agentWorkerCount agent
        if actual == expected
            then pure ()
            else threadDelay 50_000 >> go (remaining - 1)

gatedResponder :: MVar () -> MVar () -> Socket -> IO ()
gatedResponder started releaseGate sock = do
    sendHeaders sock "200 OK" 200 []
    Socket.sendAll sock (BS.replicate 100 97)
    putMVar started ()
    takeMVar releaseGate
    Socket.sendAll sock (BS.replicate 100 98)

drainBody :: BodyReader -> IO (ByteString, Either CurlCode ())
drainBody reader = go []
  where
    go chunks =
        readBody reader >>= \case
            Left curlCode -> pure (BS.concat $ reverse chunks, Left curlCode)
            Right Nothing -> pure (BS.concat $ reverse chunks, Right ())
            Right (Just chunk) -> go (chunk : chunks)

shortResponder :: Socket -> IO ()
shortResponder sock = do
    sendHeaders sock "200 OK" 10 []
    Socket.sendAll sock "short"

sendForever :: Socket -> IO ()
sendForever sock = do
    sendHeaders sock "200 OK" 1_000_000 []
    Socket.sendAll sock "first"
    let loop = threadDelay 20_000 >> Socket.sendAll sock "more" >> loop
    void (try @SomeException loop)

withServer :: (Socket -> IO ()) -> (ByteString -> IO a) -> IO a
withServer responder action = bracket openServer close \listener -> do
    port <- socketPort listener
    server <- async do
        (connection, _) <- accept listener
        finally
            (receiveRequest connection >> responder connection)
            (close connection)
    let url = BSC.pack $ "http://127.0.0.1:" <> show port <> "/"
    action url `finally` stopServer server

openServer :: IO Socket
openServer = do
    listener <- socket AF_INET Stream defaultProtocol
    setSocketOption listener ReuseAddr 1
    bind listener $ SockAddrInet 0 (tupleToHostAddress (127, 0, 0, 1))
    listen listener 1
    pure listener

stopServer :: Async () -> IO ()
stopServer server = cancel server >> void (waitCatch server)

receiveRequest :: Socket -> IO ()
receiveRequest sock = go ""
  where
    go received
        | "\r\n\r\n" `BS.isInfixOf` received = pure ()
        | otherwise = do
            chunk <- Socket.recv sock 4_096
            if BS.null chunk
                then throwIO $ userError "client closed before sending request headers"
                else go $ received <> chunk

sendFixedResponse :: ByteString -> [(ByteString, ByteString)] -> ByteString -> Socket -> IO ()
sendFixedResponse status extraHeaders responseBody sock = do
    sendHeaders sock status (BS.length responseBody) extraHeaders
    Socket.sendAll sock responseBody

sendHeaders :: Socket -> ByteString -> Int -> [(ByteString, ByteString)] -> IO ()
sendHeaders sock status contentLength extraHeaders =
    Socket.sendAll sock . BS.concat $
        [ "HTTP/1.1 "
        , status
        , "\r\nContent-Length: "
        , BSC.pack $ show contentLength
        , "\r\nConnection: close\r\n"
        , BS.concat $ fmap (\(name, value) -> name <> ": " <> value <> "\r\n") extraHeaders
        , "\r\n"
        ]
