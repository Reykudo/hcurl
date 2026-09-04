{-# LANGUAGE AllowAmbiguousTypes #-}

module Main (main) where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Concurrent.Async (Async, async, cancel, forConcurrently, poll, wait, waitCatch)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, readMVar, takeMVar, tryPutMVar)
import Control.Exception (Exception, MaskingState (Unmasked), SomeException, bracket, finally, getMaskingState, throwIO, try)
import Control.Monad (forM_, replicateM_, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BSL
import Data.Char (toLower)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List (isInfixOf)
import Data.List.NonEmpty qualified as NonEmpty
import HCurl.Internal.Agent (
    Agent (..),
    AgentClosed (..),
    AgentContext (..),
    AgentHandle (..),
    ManagedAgent (..),
    ManagedMetrics (..),
    ManagedPolicy (..),
    ManagedState (..),
    ManagedWorker (..),
    TransferIdExhausted,
    agentWorkerCount,
    closeAgent,
    newTransferId,
    registerManagedMetrics,
    sendMessage,
    spawnAgent,
    spawnManagedAgent,
    spawnThreadedAgent,
    stopAgent,
    withAgent,
    withManagedAgent,
    withThreadedAgent,
 )
import HCurl.Internal.Body (markUploadClosed, newUploadBodyState, writeUploadChunk)
import HCurl.Internal.MPSC (InvalidMPSCQueueCapacity, initMPSCQ)
import HCurl.Internal.Metrics (Metrics (..))
import HCurl.Internal.Options (InvalidOptionValue, SomeOption (..))
import HCurl.Internal.Raw (CurlCode (..))
import HCurl.Internal.Raw.MPSC (OuterMessage (..), TransferId (..))
import HCurl.Internal.Slist (CurlSlistError, toHeaderSlist)
import HCurl.Request (InvalidRequest, LowSpeedLimit (..), Request (..), RequestHeader (..))
import HCurl.Request qualified as Request
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
import HCurl.Types (AgentConfig (..), Body (Buffer, Empty), HTTPMethod (..), defaultConfig)
import HCurl.Upload (
    StreamingUploadUnsupported (..),
    UploadConfig (..),
    abortBody,
    endBody,
    feedBody,
    httpUpload,
    httpUploadWith,
    streamingUploadSupported,
 )
import Network.Socket
import Network.Socket.ByteString qualified as Socket
import System.Environment (lookupEnv)
import System.IO (hPutStrLn, stderr)
import System.Mem (performMajorGC)
import System.Timeout qualified as Timeout

main :: IO ()
main = withSocketsDo do
    initCurl
    checkDefaultConfig
    runTest "compatibility: unknown runtime curl codes remain representable" testUnknownCurlCode
    uploadSupported <- streamingUploadSupported
    withThreadedAgent 4 defaultConfig \agent -> do
        runTest "threaded: concurrent buffered requests complete with correct bodies" $ testConcurrentBuffered agent
        runTest "threaded: failing transfers do not poison the agent" $ testFailureIsolation agent
        runTest "threaded: streaming backpressure pauses and resumes libcurl" $ testBackpressure agent
        runTest "threaded: early close cancels the transfer and the agent stays usable" $ testEarlyCancel agent
        runTest "threaded: close racing a body callback reports cancellation" $ testCloseCallbackRace agent
        runTest "threaded: truncated response reports a late curl error" $ testLateFailure agent
        runTest "request: every buffered method sends its body" $ testBufferedMethods agent
        runTest "headers: informational and redirect blocks are not mixed" $ testFinalHeaderBlocks agent
        runTest "headers: streaming response is returned before the first body byte" $ testHeadersBeforeBody agent
        runTest "headers: OverrideHeaders survives a major GC" $ testOverrideHeadersLifetime agent
        runTest "headers: an empty reusable list behaves like no headers" $ testEmptyOverrideHeaders agent
        runTest "request: malformed values and options are rejected before submit" $ testRequestValidation agent
        runTest "redirects: buffered requests follow unless explicitly disabled" $ testRedirectPolicy agent
        if uploadSupported
            then do
                runTest "upload: chunked POST body is delivered byte-for-byte" $ testUploadBody agent
                runTest "upload: reusable headers survive the request-local overlay" $ testUploadOverrideHeaders agent
                runTest "upload: backpressure throttles the producer and still completes" $ testUploadBackpressure agent
                runTest "upload: producer abort fails the transfer deterministically" $ testUploadAbort agent
                runTest "upload: explicit EOF remains successful after fast completion" $ testUploadRepeatedEnd agent
                runTest "upload: streaming POST does not replay across 307 or 308" $ testUploadRedirects agent
            else runTest "upload: curl 7.18 rejects its unsafe pause implementation" $ testUploadUnsupported agent
        runTest "upload: zero buffered upload chunks is rejected up front" $ testUploadZeroChunks agent
        runTest "upload: wrong method or pre-set body is rejected" $ testUploadValidation agent
        runTest "streaming: abandoned reader becomes a deterministic error after scope exit" $ testAbandonedScopeExit agent
        runTest "streaming: scoped API closes the body on early return" $ testScopedEarlyReturn agent
        runTest "streaming: outer handler scope releases several abandoned requests" $ testHandlerScope agent
        runTest "streaming: blocked reader is woken when the stream is released" $ testBlockedReaderWoken agent
        runTest "streaming: an async-cancelled waiter gives StablePtr ownership back" $ testCancelledReaderWait agent
        runTest "streaming: zero buffered chunks is rejected up front" $ testZeroBufferConfig agent
    withManagedAgent (fastPolicy 2) defaultConfig \managedAgent ->
        runTest "managed: pool scales by measured demand and shrinks under sustained light traffic" $ testManagedDemandScaling managedAgent
    withManagedAgent (fastPolicy 2) defaultConfig \streamingAgent ->
        runTest "managed: streaming leases drain and the pool shrinks under traffic" $ testManagedStreamingScaling streamingAgent
    withManagedAgent (fastPolicy 2) defaultConfig \metricsAgent ->
        runTest "managed: metrics hook reports running agents and demand" $ testManagedMetricsHook metricsAgent
    withManagedAgent (fastPolicy 2) defaultConfig \managedAbandonAgent ->
        runTest "managed: abandoned streams release their leases at scope exit" $ testManagedAbandonedLeases managedAbandonAgent
    runTest "lifecycle: close is concurrent, idempotent, and rejects new requests" testConcurrentClose
    runTest "lifecycle: a threaded pool skips a stopped worker" testThreadedSkipsStoppedWorker
    runTest "lifecycle: close aborts active buffered transfers" testCloseActiveBuffered
    runTest "lifecycle: close aborts active download streams" testCloseActiveDownload
    when uploadSupported $
        runTest "lifecycle: close aborts active upload streams" testCloseActiveUpload
    runTest "lifecycle: cancelling a buffered request cancels its transfer" testCancelledBufferedRequest
    runTest "lifecycle: cancelling a response-head waiter is safe" testCancelledHeaderWait
    runTest "lifecycle: cancelling a completion waiter leaves the stream usable" testCancelledCompletionWait
    runTest "lifecycle: cancelling an upload writer waiter is safe" testCancelledWriterWait
    runTest "lifecycle: an unsubmitted stream closes after its agent" testUnsubmittedStreamAfterAgentClose
    runTest "lifecycle: managed close terminates controller and workers" testManagedClose
    runTest "lifecycle: managed pool replaces a stopped worker" testManagedReplacesStoppedWorker
    runTest "lifecycle: managed close owns workers pending retirement" testManagedCloseOwnsRetiringWorkers
    runTest "lifecycle: managed close interrupts a blocked metrics hook" testManagedCloseBlockedHook
    runTest "lifecycle: stale resume and cancel messages are harmless" testStaleControlMessages
    runTest "lifecycle: duplicate transfer IDs do not detach active requests" testDuplicateTransferIdIsolation
    runTest "lifecycle: exhausted transfer IDs are never reused" testTransferIdExhaustion
    runTest "mpsc: non-power-of-two capacity is rejected" testMPSCValidation

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

testUnknownCurlCode :: IO ()
testUnknownCurlCode = do
    assertEqual "known curl code number" 42 (fromEnum AbortedByCallback)
    assertEqual "known curl code decode" PeerFailedVerification (toEnum 60)
    assertEqual "future curl code decode" (UnknownCurlCode 4_096) (toEnum 4_096)
    assertEqual "future curl code round-trip" 4_096 (fromEnum $ toEnum @CurlCode 4_096)

runTest :: String -> IO () -> IO ()
runTest name action = do
    selected <- maybe True (`isInfixOf` name) <$> lookupEnv "HCURL_TEST_MATCH"
    when selected do
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
        { url
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
                    unless (payload == actual) . throwIO . userError $
                        "body after pause/resume: expected "
                            <> show (BS.length payload)
                            <> " bytes, got "
                            <> show (BS.length actual)
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

testCloseCallbackRace :: Agent -> IO ()
testCloseCallbackRace agent =
    replicateM_ 64 $
        withServer floodResponder \url ->
            runResourceT do
                result <- httpStreamingWith StreamConfig{bufferedChunks = 64} agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("response did not start: " <> show curlCode)
                    Right StreamingResponse{body = reader, completion} -> liftIO do
                        readBody reader >>= \case
                            Right (Just _) -> pure ()
                            other -> throwIO $ userError $ "expected first flood chunk, got " <> show other
                        closeBody reader
                        completion >>= assertEqual "racing close completion" (Left AbortedByCallback)
  where
    floodResponder sock = do
        sendHeaders sock "200 OK" (64 * 1024 * 1024) []
        let chunk = BS.replicate (64 * 1024) 120
            send = Socket.sendAll sock chunk >> send
        void $ try @SomeException send

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

testBufferedMethods :: Agent -> IO ()
testBufferedMethods agent =
    forM_ methods $ \(httpMethod, expectedMethod) -> do
        let payload = "body-" <> expectedMethod <> "-\NUL-binary"
            request = (requestFor "unused"){method = httpMethod, body = Buffer payload}
        withRawServer echoMethodAndBody \url -> do
            result <- runResourceT $ httpLBS agent request{url = url}
            assertBufferedOk
                ("buffered " <> BSC.unpack expectedMethod)
                (expectedMethod <> "\n" <> payload)
                result
  where
    methods =
        [ (Get, "GET")
        , (Post, "POST")
        , (Put, "PUT")
        , (Delete, "DELETE")
        , (Patch, "PATCH")
        , (Custom "MERGE", "MERGE")
        ]

    echoMethodAndBody sock = do
        (headerBlock, initialBody) <- receiveRequestHeaders sock
        requestBody <- readRequestBody sock headerBlock initialBody
        let requestMethod = BS.takeWhile (/= 32) headerBlock
        sendFixedResponse "200 OK" [] (requestMethod <> "\n" <> requestBody) sock

testFinalHeaderBlocks :: Agent -> IO ()
testFinalHeaderBlocks agent = do
    withServer interimResponder \url -> do
        result <- runResourceT $ httpLBS agent (requestFor url)
        case result of
            Left curlCode -> throwIO $ userError $ "informational response failed: " <> show curlCode
            Right Response{info = HttpParts{statusCode, headers}, body} -> do
                assertEqual "informational final status" 200 statusCode
                assertEqual "informational final body" "final" body
                unless (("X-Final", "yes") `elem` headers) $
                    throwIO $
                        userError "final response header is missing"
                when (("X-Interim", "discard-me") `elem` headers) $
                    throwIO $
                        userError "1xx headers leaked into the final block"

    withServer (sendFixedResponse "200 OK" [("X-Final", "redirected")] "redirected") \finalUrl ->
        withServer (redirectResponder "302 Found" finalUrl) \redirectUrl -> do
            result <- runResourceT $ httpLBS agent (requestFor redirectUrl)
            case result of
                Left curlCode -> throwIO $ userError $ "redirect failed: " <> show curlCode
                Right Response{info = HttpParts{statusCode, headers}, body} -> do
                    assertEqual "redirect final status" 200 statusCode
                    assertEqual "redirect final body" "redirected" body
                    unless (("X-Final", "redirected") `elem` headers) $
                        throwIO $
                            userError "redirect final header is missing"
                    when (("X-Redirect", "discard-me") `elem` headers) $
                        throwIO $
                            userError "redirect headers leaked into the final block"

    failedDestination <- closedLocalUrl
    withServer (redirectResponder "302 Found" failedDestination) \redirectUrl ->
        runResourceT (httpStreaming agent $ requestFor redirectUrl) >>= \case
            Left _ -> pure ()
            Right response -> do
                closeBody response.body
                throwIO $ userError "failed redirect exposed its intermediate 302 response"

    withServer (sendFixedResponse "200 OK" [("X-Final", "streamed")] "final-stream") \finalUrl ->
        withServer (redirectResponderWithBody finalUrl $ BS.replicate 131_072 114) \redirectUrl ->
            runResourceT do
                result <- httpStreamingWith StreamConfig{bufferedChunks = 1} agent (requestFor redirectUrl)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError $ "streaming redirect failed: " <> show curlCode
                    Right StreamingResponse{info = HttpParts{statusCode, headers}, body = reader} -> liftIO do
                        assertEqual "streaming redirect final status" 200 statusCode
                        unless (("X-Final", "streamed") `elem` headers) $
                            throwIO $
                                userError "streaming redirect published an intermediate header block"
                        (actual, streamResult) <- drainBody reader
                        assertEqual "streaming redirect final body" "final-stream" actual
                        assertEqual "streaming redirect completion" (Right ()) streamResult

    let authBody = BS.replicate 131_072 97
    withServer (sendFixedResponse "401 Unauthorized" [("WWW-Authenticate", "Basic")] authBody) \url ->
        within 2_000_000 $
            runResourceT do
                result <- httpStreamingWith StreamConfig{bufferedChunks = 1} agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError $ "401 stream failed: " <> show curlCode
                    Right StreamingResponse{info = HttpParts{statusCode}, body = reader} -> liftIO do
                        assertEqual "401 status" 401 statusCode
                        (actual, streamResult) <- drainBody reader
                        assertEqual "401 body" authBody actual
                        assertEqual "401 completion" (Right ()) streamResult
  where
    interimResponder sock =
        Socket.sendAll
            sock
            "HTTP/1.1 100 Continue\r\nX-Interim: discard-me\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-Final: yes\r\nConnection: close\r\n\r\nfinal"

redirectResponderWithBody :: ByteString -> ByteString -> Socket -> IO ()
redirectResponderWithBody destination =
    sendFixedResponse
        "302 Found"
        [("Location", destination), ("X-Redirect", "discard-me")]

testHeadersBeforeBody :: Agent -> IO ()
testHeadersBeforeBody agent = do
    releaseBody <- newEmptyMVar
    withServer (delayedBodyResponder releaseBody) \url ->
        within 2_000_000 $
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError $ "streaming head failed: " <> show curlCode
                    Right StreamingResponse{info = HttpParts{statusCode}, body = reader, completion} -> liftIO do
                        assertEqual "early status" 200 statusCode
                        putMVar releaseBody ()
                        (actual, streamResult) <- drainBody reader
                        assertEqual "delayed body" "late" actual
                        assertEqual "delayed body result" (Right ()) streamResult
                        void completion
  where
    delayedBodyResponder releaseBody sock = do
        sendHeaders sock "200 OK" 4 [("X-Head", "early")]
        void $ takeMVar releaseBody
        Socket.sendAll sock "late"

testOverrideHeadersLifetime :: Agent -> IO ()
testOverrideHeadersLifetime agent = do
    capturedHeaders <- newEmptyMVar
    releaseResponse <- newEmptyMVar
    slist <- toHeaderSlist ["X-HCurl-Lifetime: retained"]
    let request = (requestFor "unused"){Request.headers = OverrideHeaders slist}
    withRawServer (captureResponder capturedHeaders releaseResponse) \url -> do
        response <- async $ runResourceT $ httpLBS agent request{url = url}
        replicateM_ 3 performMajorGC
        headerBlock <- takeMVar capturedHeaders
        unless ("x-hcurl-lifetime: retained" `BSC.isInfixOf` BSC.map toLower headerBlock) $
            throwIO $
                userError "OverrideHeaders disappeared before transfer completion"
        putMVar releaseResponse ()
        wait response >>= assertBufferedOk "OverrideHeaders lifetime" "ok"
  where
    captureResponder capturedHeaders releaseResponse sock = do
        threadDelay 200_000
        (headerBlock, _) <- receiveRequestHeaders sock
        putMVar capturedHeaders headerBlock
        void $ takeMVar releaseResponse
        sendFixedResponse "200 OK" [] "ok" sock

testEmptyOverrideHeaders :: Agent -> IO ()
testEmptyOverrideHeaders agent = do
    slist <- toHeaderSlist []
    withServer (sendFixedResponse "200 OK" [] "empty-slist") \url ->
        runResourceT
            (httpLBS agent (requestFor url){Request.headers = OverrideHeaders slist})
            >>= assertBufferedOk "empty reusable header list" "empty-slist"

testRequestValidation :: Agent -> IO ()
testRequestValidation agent = do
    let base = requestFor "http://127.0.0.1:1/"
        invalidRequests =
            [ ("empty URL", base{url = ""})
            , ("NUL in URL", base{url = "http://example.invalid/\NULtail"})
            , ("newline in URL", base{url = "http://example.invalid/\nheader"})
            , ("space in URL", base{url = "http://example.invalid/a b"})
            , ("negative timeout", base{timeoutMS = -1})
            , ("negative connect timeout", base{connectionTimeoutMS = -1})
            , ("negative low-speed time", base{lowSpeedLimit = LowSpeedLimit{lowSpeed = 1, timeout = -1}})
            , ("negative low-speed bytes", base{lowSpeedLimit = LowSpeedLimit{lowSpeed = -1, timeout = 1}})
            , ("empty custom method", base{method = Custom ""})
            , ("invalid custom method", base{method = Custom "BAD METHOD"})
            , ("NUL in header", base{Request.headers = HeaderList ["X-Bad: before\NULafter"]})
            , ("newline in header", base{Request.headers = HeaderList ["X-Bad: before\r\nafter"]})
            , ("HEAD body", base{method = Head, body = Buffer "forbidden"})
            ]
    forM_ invalidRequests $ \(label, request) ->
        assertThrows @InvalidRequest label $ runResourceT $ httpLBS agent request
    assertThrows @InvalidOptionValue "negative option" $
        runResourceT $
            httpLBS agent base{extraOptions = [OptionTimeoutMs (-1)]}
    assertThrows @InvalidOptionValue "NUL option" $
        runResourceT $
            httpLBS agent base{extraOptions = [OptionAcceptEncoding "gzip\NULbr"]}
    assertThrows @InvalidOptionValue "newline option" $
        runResourceT $
            httpLBS agent base{extraOptions = [OptionAcceptEncoding "gzip\r\nX-Bad"]}
    assertThrows @CurlSlistError "newline in reusable header list" $
        void $
            toHeaderSlist ["X-Bad: before\r\nafter"]

testRedirectPolicy :: Agent -> IO ()
testRedirectPolicy agent =
    withServer (sendFixedResponse "200 OK" [] "destination") \finalUrl -> do
        withServer (redirectResponder "302 Found" finalUrl) \redirectUrl -> do
            followed <- runResourceT $ httpLBS agent (requestFor redirectUrl)
            assertBufferedOk "default redirect" "destination" followed
        withServer (redirectResponder "302 Found" finalUrl) \redirectUrl -> do
            notFollowed <-
                runResourceT $
                    httpLBS agent (requestFor redirectUrl){extraOptions = [OptionFollowLocation False]}
            case notFollowed of
                Left curlCode -> throwIO $ userError $ "disabled redirect failed: " <> show curlCode
                Right Response{info = HttpParts{statusCode}, body} -> do
                    assertEqual "disabled redirect status" 302 statusCode
                    assertEqual "disabled redirect body" "redirect" body

redirectResponder :: ByteString -> ByteString -> Socket -> IO ()
redirectResponder status destination =
    sendFixedResponse status [("Location", destination), ("X-Redirect", "discard-me")] "redirect"

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
lightTraffic agent = go
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
    hookCalls <- newIORef (0 :: Int)
    controllerMaskingState <- newEmptyMVar
    registerManagedMetrics agent $ \snapshot -> do
        atomicModifyIORef' recorder (\snapshots -> (snapshot : snapshots, ()))
        call <- atomicModifyIORef' hookCalls $ \count -> let next = count + 1 in (next, next)
        when (call >= 2) $ getMaskingState >>= void . tryPutMVar controllerMaskingState
    within 2_000_000 (takeMVar controllerMaskingState)
        >>= assertEqual "managed metrics hook masking state" Unmasked
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

testCancelledReaderWait :: Agent -> IO ()
testCancelledReaderWait agent = do
    started <- newEmptyMVar
    releaseGate <- newEmptyMVar
    withServer (gatedResponder started releaseGate) \url ->
        runResourceT do
            result <- httpStreaming agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError $ "stream did not start: " <> show curlCode
                Right StreamingResponse{body = reader} -> liftIO do
                    first <- readBody reader
                    assertEqual "first chunk before waiter cancellation" (Right $ Just $ BS.replicate 100 97) first
                    waiter <- async $ readBody reader
                    threadDelay 100_000
                    cancel waiter
                    waitCatch waiter >>= \case
                        Left _ -> pure ()
                        Right value -> throwIO $ userError $ "cancelled reader unexpectedly returned " <> show value
                    closeBody reader
                    void $ tryPutMVar releaseGate ()

testConcurrentClose :: IO ()
testConcurrentClose = do
    agent <- spawnThreadedAgent 4 defaultConfig
    flip finally (closeAgent agent) do
        void $ forConcurrently [1 .. 32 :: Int] $ const $ closeAgent agent
        closeAgent agent
        agentWorkerCount agent >>= assertEqual "workers after threaded close" (Just 0)
        assertThrows @AgentClosed "request after close" $
            runResourceT $
                httpLBS agent (requestFor "http://127.0.0.1:1/")

testThreadedSkipsStoppedWorker :: IO ()
testThreadedSkipsStoppedWorker =
    withThreadedAgent 2 defaultConfig \agent -> case agent of
        Threaded handles _ _
            | _stopped : _survivor : _ <- NonEmpty.toList handles -> do
                stopAgent _stopped
                replicateM_ 4 $
                    withServer (sendFixedResponse "200 OK" [] "survivor") \url ->
                        runResourceT (httpLBS agent (requestFor url))
                            >>= assertBufferedOk "threaded survivor" "survivor"
        _ -> pure ()

testCloseActiveBuffered :: IO ()
testCloseActiveBuffered =
    withAgent defaultConfig \agent -> do
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \url -> do
            request <- async $ runResourceT $ httpLBS agent (requestFor url)
            outcome <-
                ( do
                    takeMVar started
                    closeAgent agent
                    wait request
                )
                    `finally` void (tryPutMVar releaseGate ())
            assertCurlFailure "buffered close" AbortedByCallback outcome

testCloseActiveDownload :: IO ()
testCloseActiveDownload =
    withAgent defaultConfig \agent -> do
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \url ->
            ( runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError $ "stream did not start: " <> show curlCode
                    Right StreamingResponse{body = reader, completion} -> liftIO do
                        first <- readBody reader
                        assertEqual "download before agent close" (Right $ Just $ BS.replicate 100 97) first
                        closeAgent agent
                        readBody reader >>= assertEqual "download after agent close" (Left AbortedByCallback)
                        completion >>= assertEqual "download completion after agent close" (Left AbortedByCallback)
            )
                `finally` void (tryPutMVar releaseGate ())

testCloseActiveUpload :: IO ()
testCloseActiveUpload =
    withAgent defaultConfig \agent -> do
        serverStarted <- newEmptyMVar
        producerStarted <- newEmptyMVar
        releaseServer <- newEmptyMVar
        releaseProducer <- newEmptyMVar
        withRawServer (stalledUploadResponder serverStarted releaseServer) \url -> do
            request <-
                async $
                    runResourceT $
                        httpUpload agent (uploadRequestFor url) \body -> liftIO do
                            feedBody body "in-flight" >>= assertEqual "initial upload feed" (Right ())
                            putMVar producerStarted ()
                            takeMVar releaseProducer
            outcome <-
                ( do
                    takeMVar serverStarted
                    takeMVar producerStarted
                    closeAgent agent
                    putMVar releaseProducer ()
                    wait request
                )
                    `finally` do
                        void $ tryPutMVar releaseProducer ()
                        void $ tryPutMVar releaseServer ()
            assertCurlFailure "upload close" AbortedByCallback outcome
  where
    stalledUploadResponder started release sock = do
        _ <- receiveRequestHeaders sock
        putMVar started ()
        takeMVar release

testCancelledBufferedRequest :: IO ()
testCancelledBufferedRequest =
    withAgent defaultConfig \agent -> do
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \url ->
            ( do
                requester <- async $ runResourceT $ httpLBS agent (requestFor url)
                takeMVar started
                within 2_000_000 $ cancel requester
                waitCatch requester >>= \case
                    Left _ -> pure ()
                    Right _ -> throwIO $ userError "cancelled buffered request unexpectedly returned"
            )
                `finally` void (tryPutMVar releaseGate ())
        withServer (sendFixedResponse "200 OK" [] "still-alive") \url ->
            runResourceT (httpLBS agent (requestFor url))
                >>= assertBufferedOk "after cancelled buffered request" "still-alive"

testCancelledHeaderWait :: IO ()
testCancelledHeaderWait =
    withAgent defaultConfig \agent -> do
        requestSeen <- newEmptyMVar
        releaseHeaders <- newEmptyMVar
        withServer (delayedHeadersResponder requestSeen releaseHeaders) \url -> do
            requester <- async $ runResourceT $ httpStreaming agent (requestFor url)
            takeMVar requestSeen
            threadDelay 100_000
            cancel requester
            waitCatch requester >>= \case
                Left _ -> pure ()
                Right _ -> throwIO $ userError "cancelled response-head waiter unexpectedly returned"
            void $ tryPutMVar releaseHeaders ()
        withServer (sendFixedResponse "200 OK" [] "still-alive") \url ->
            runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after cancelled head waiter" "still-alive"
  where
    delayedHeadersResponder requestSeen releaseHeaders sock = do
        putMVar requestSeen ()
        void $ takeMVar releaseHeaders
        sendFixedResponse "200 OK" [] "too-late" sock

testCancelledCompletionWait :: IO ()
testCancelledCompletionWait =
    withAgent defaultConfig \agent -> do
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \url ->
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError $ "stream did not start: " <> show curlCode
                    Right StreamingResponse{body = reader, completion} -> liftIO do
                        takeMVar started
                        waiter <- async completion
                        threadDelay 100_000
                        within 2_000_000 $ cancel waiter
                        waitCatch waiter >>= \case
                            Left _ -> pure ()
                            Right _ -> throwIO $ userError "cancelled completion waiter unexpectedly returned"
                        putMVar releaseGate ()
                        (actual, streamResult) <- drainBody reader
                        assertEqual
                            "body after completion waiter cancellation"
                            (BS.replicate 100 97 <> BS.replicate 100 98)
                            actual
                        assertEqual "stream after completion waiter cancellation" (Right ()) streamResult
                        completion >>= \case
                            Left curlCode -> throwIO $ userError $ "completion retry failed: " <> show curlCode
                            Right _ -> pure ()

testCancelledWriterWait :: IO ()
testCancelledWriterWait =
    withAgent defaultConfig \agent -> do
        context <- case agent of
            Single AgentHandle{agentContext} -> pure agentContext
            _ -> throwIO $ userError "spawnAgent returned a non-single agent"
        uploadState <- newUploadBodyState 1 context (TransferId maxBound)
        writeUploadChunk uploadState "fills-the-only-slot" >>= assertEqual "initial raw upload write" (Right ())
        waiter <- async $ writeUploadChunk uploadState "must-block"
        threadDelay 100_000
        cancel waiter
        waitCatch waiter >>= \case
            Left _ -> pure ()
            Right value -> throwIO $ userError $ "cancelled upload writer unexpectedly returned " <> show value
        markUploadClosed uploadState

testUnsubmittedStreamAfterAgentClose :: IO ()
testUnsubmittedStreamAfterAgentClose = do
    agent <- spawnAgent defaultConfig
    context <- case agent of
        Single AgentHandle{agentContext} -> pure agentContext
        _ -> throwIO $ userError "spawnAgent returned a non-single agent"
    uploadState <- newUploadBodyState 1 context (TransferId maxBound)
    closeAgent agent
    within 2_000_000 $ markUploadClosed uploadState
    writeUploadChunk uploadState "after-close"
        >>= assertEqual "write to stream after agent close" (Left AbortedByCallback)

testManagedClose :: IO ()
testManagedClose = do
    agent <- spawnManagedAgent (fastPolicy 2) defaultConfig
    flip finally (closeAgent agent) do
        managed <- case agent of
            Managed value -> pure value
            _ -> throwIO $ userError "spawnManagedAgent returned a non-managed agent"
        controller <- readMVar managed.maController
        void $ forConcurrently [1 .. 16 :: Int] $ const $ closeAgent agent
        agentWorkerCount agent >>= assertEqual "managed workers after close" (Just 0)
        poll controller >>= \case
            Nothing -> throwIO $ userError "managed controller is still running after close"
            Just _ -> pure ()
        assertThrows @AgentClosed "managed request after close" $
            runResourceT $
                httpLBS agent (requestFor "http://127.0.0.1:1/")

testManagedReplacesStoppedWorker :: IO ()
testManagedReplacesStoppedWorker = do
    agent <- spawnManagedAgent (fastPolicy 1) defaultConfig
    flip finally (closeAgent agent) do
        managed <- case agent of
            Managed value -> pure value
            _ -> throwIO $ userError "spawnManagedAgent returned a non-managed agent"
        state <- readMVar managed.maState
        case state.msWorkers of
            worker : _ -> stopAgent worker.mwHandle
            [] -> throwIO $ userError "managed agent started without a worker"
        waitForWorkerCount "managed replacement" agent (Just 1)
        withServer (sendFixedResponse "200 OK" [] "replacement") \url ->
            runResourceT (httpLBS agent (requestFor url))
                >>= assertBufferedOk "managed replacement request" "replacement"

testManagedCloseOwnsRetiringWorkers :: IO ()
testManagedCloseOwnsRetiringWorkers = do
    agent <- spawnManagedAgent (fastPolicy 1) defaultConfig
    releaseHook <- newEmptyMVar
    flip finally (void $ tryPutMVar releaseHook () >> closeAgent agent) do
        managed <- case agent of
            Managed value -> pure value
            _ -> throwIO $ userError "spawnManagedAgent returned a non-managed agent"
        original <- do
            state <- readMVar managed.maState
            case state.msWorkers of
                worker : _ -> pure worker
                [] -> throwIO $ userError "managed agent started without a worker"
        calls <- newIORef (0 :: Int)
        hookEntered <- newEmptyMVar
        registerManagedMetrics agent $ \_ -> do
            call <- atomicModifyIORef' calls $ \count -> let next = count + 1 in (next, next)
            when (call >= 2) do
                void $ tryPutMVar hookEntered ()
                void $ takeMVar releaseHook
        writeIORef original.mwQuiescing True
        within 2_000_000 $ takeMVar hookEntered
        within 2_000_000 $ closeAgent agent
        poll original.mwHandle.agentThreadId >>= \case
            Nothing -> throwIO $ userError "retiring worker survived managed close"
            Just _ -> pure ()
        agentWorkerCount agent >>= assertEqual "workers after close during retirement" (Just 0)

testManagedCloseBlockedHook :: IO ()
testManagedCloseBlockedHook = do
    agent <- spawnManagedAgent (fastPolicy 1) defaultConfig
    releaseHook <- newEmptyMVar
    flip finally (void $ tryPutMVar releaseHook () >> closeAgent agent) do
        managed <- case agent of
            Managed value -> pure value
            _ -> throwIO $ userError "spawnManagedAgent returned a non-managed agent"
        controller <- readMVar managed.maController
        calls <- newIORef (0 :: Int)
        hookEntered <- newEmptyMVar
        registerManagedMetrics agent $ \_ -> do
            call <- atomicModifyIORef' calls $ \count -> let next = count + 1 in (next, next)
            when (call >= 2) do
                void $ tryPutMVar hookEntered ()
                void $ takeMVar releaseHook
        takeMVar hookEntered
        closer <- async $ closeAgent agent
        within 2_000_000 $ wait closer
        agentWorkerCount agent >>= assertEqual "workers after blocked-hook close" (Just 0)
        poll controller >>= \case
            Nothing -> throwIO $ userError "managed controller survived blocked-hook close"
            Just _ -> pure ()

testStaleControlMessages :: IO ()
testStaleControlMessages =
    withAgent defaultConfig \agent -> do
        context <- case agent of
            Single AgentHandle{agentContext} -> pure agentContext
            _ -> throwIO $ userError "spawnAgent returned a non-single agent"
        withServer (sendFixedResponse "200 OK" [] "first") \url ->
            runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "before stale messages" "first"
        sendMessage context $ ResumeRequest (TransferId 1)
        sendMessage context $ CancelRequest (TransferId 1)
        withServer (sendFixedResponse "200 OK" [] "second") \url ->
            runResourceT (httpLBS agent (requestFor url)) >>= assertBufferedOk "after stale messages" "second"

testDuplicateTransferIdIsolation :: IO ()
testDuplicateTransferIdIsolation =
    withAgent defaultConfig \agent -> do
        context <- case agent of
            Single AgentHandle{agentContext} -> pure agentContext
            _ -> throwIO $ userError "spawnAgent returned a non-single agent"
        started <- newEmptyMVar
        releaseGate <- newEmptyMVar
        withServer (gatedResponder started releaseGate) \firstUrl -> do
            first <- async $ runResourceT $ httpLBS agent (requestFor firstUrl)
            takeMVar started
            writeIORef context.nextId 0
            withServer (sendFixedResponse "200 OK" [] "must-not-run") \secondUrl -> do
                second <- runResourceT $ httpLBS agent (requestFor secondUrl)
                assertCurlFailure "duplicate transfer ID" FailedInit second
            putMVar releaseGate ()
            wait first >>= assertBufferedOk "original transfer after duplicate ID" (BS.replicate 100 97 <> BS.replicate 100 98)

testTransferIdExhaustion :: IO ()
testTransferIdExhaustion =
    withAgent defaultConfig \agent -> do
        context <- case agent of
            Single AgentHandle{agentContext} -> pure agentContext
            _ -> throwIO $ userError "spawnAgent returned a non-single agent"
        writeIORef context.nextId maxBound
        replicateM_ 2 $
            assertThrows @TransferIdExhausted "transfer ID exhaustion" $
                newTransferId context

testMPSCValidation :: IO ()
testMPSCValidation =
    assertThrows @InvalidMPSCQueueCapacity "non-power-of-two MPSC capacity" $
        initMPSCQ 3

assertThrows :: forall exception value. (Exception exception) => String -> IO value -> IO ()
assertThrows label action =
    try @exception action >>= \case
        Left _ -> pure ()
        Right _ -> throwIO $ userError $ label <> ": expected exception"

assertCurlFailure :: String -> CurlCode -> Either CurlCode value -> IO ()
assertCurlFailure label expected = \case
    Left actual -> assertEqual label expected actual
    Right _ -> throwIO $ userError $ label <> ": unexpectedly succeeded"

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

withRawServer :: (Socket -> IO ()) -> (ByteString -> IO a) -> IO a
withRawServer responder action = bracket openServer close \listener -> do
    port <- socketPort listener
    server <- async do
        (connection, _) <- accept listener
        finally (responder connection) (close connection)
    let url = BSC.pack $ "http://127.0.0.1:" <> show port <> "/"
    action url `finally` stopServer server

openServer :: IO Socket
openServer = do
    listener <- socket AF_INET Stream defaultProtocol
    setSocketOption listener ReuseAddr 1
    bind listener $ SockAddrInet 0 (tupleToHostAddress (127, 0, 0, 1))
    listen listener 1
    pure listener

closedLocalUrl :: IO ByteString
closedLocalUrl =
    bracket openServer close \listener -> do
        port <- socketPort listener
        pure . BSC.pack $ "http://127.0.0.1:" <> show port <> "/"

stopServer :: Async () -> IO ()
stopServer server = cancel server >> void (waitCatch server)

receiveRequest :: Socket -> IO ()
receiveRequest sock = void (receiveRequestHeaders sock)

{- | Read the request header block, returning it together with any bytes of
the request body that already arrived in the same TCP segment.
-}
receiveRequestHeaders :: Socket -> IO (ByteString, ByteString)
receiveRequestHeaders sock = go BS.empty
  where
    go received =
        case BS.breakSubstring "\r\n\r\n" received of
            (headerPrefix, rest)
                | not (BS.null rest) ->
                    let headerBlock = headerPrefix <> BS.take 4 rest
                     in pure (headerBlock, BS.drop 4 rest)
            _ -> do
                chunk <- Socket.recv sock 4_096
                if BS.null chunk
                    then throwIO $ userError "client closed before sending request headers"
                    else go (received <> chunk)

readRequestBody :: Socket -> ByteString -> ByteString -> IO ByteString
readRequestBody sock headerBlock initialBody
    | "transfer-encoding: chunked" `BSC.isInfixOf` lowerHeaders =
        readChunkedUploadBody sock initialBody
    | Just contentLength <- headerInteger "content-length" headerBlock =
        fst <$> readExactBytes sock contentLength initialBody
    | otherwise = pure BS.empty
  where
    lowerHeaders = BSC.map toLower headerBlock

headerInteger :: ByteString -> ByteString -> Maybe Int
headerInteger name headerBlock = do
    line <- findHeaderLine
    let value = BSC.strip $ BS.drop 1 $ snd $ BSC.break (== ':') line
    case BSC.readInt value of
        Just (number, remainder)
            | number >= 0 && BS.null (BSC.strip remainder) -> Just number
        _ -> Nothing
  where
    expected = BSC.map toLower name
    findHeaderLine = go $ BSC.lines headerBlock
    go [] = Nothing
    go (line : rest) =
        let (key, suffix) = BSC.break (== ':') line
         in if not (BS.null suffix) && BSC.map toLower (BSC.strip key) == expected
                then Just line
                else go rest

readExactBytes :: Socket -> Int -> ByteString -> IO (ByteString, ByteString)
readExactBytes sock required = go
  where
    go buffered
        | BS.length buffered >= required =
            pure (BS.take required buffered, BS.drop required buffered)
        | otherwise = do
            more <- Socket.recv sock 8_192
            if BS.null more
                then throwIO $ userError "client closed before sending the complete request body"
                else go (buffered <> more)

-- | Decode an HTTP/1.1 chunked request body from the given starting buffer.
readChunkedUploadBody :: Socket -> ByteString -> IO ByteString
readChunkedUploadBody sock initial = go initial []
  where
    go buf acc = do
        (payloadSize, afterSizeLine) <- readSizeLine buf
        if payloadSize == 0
            then pure (BS.concat $ reverse acc)
            else do
                (payload, afterPayload) <- readExact afterSizeLine payloadSize
                afterTerminator <- skipCrlf afterPayload
                go afterTerminator (payload : acc)

    recvMore = do
        more <- Socket.recv sock 8_192
        if BS.null more
            then throwIO $ userError "client closed mid upload body"
            else pure more

    readSizeLine buf =
        case BS.breakSubstring "\r\n" buf of
            (line, rest)
                | not (BS.null rest) -> pure (chunkHexSize line, BS.drop 2 rest)
            _ -> do
                more <- recvMore
                readSizeLine (buf <> more)

    readExact buf n
        | BS.length buf >= n = pure (BS.take n buf, BS.drop n buf)
        | otherwise = do
            more <- recvMore
            readExact (buf <> more) n

    skipCrlf buf =
        case BS.stripPrefix "\r\n" buf of
            Just rest -> pure rest
            Nothing
                | BS.null buf -> do
                    more <- recvMore
                    skipCrlf more
                | Just rest <- BS.stripPrefix "\r" buf -> do
                    more <- recvMore
                    skipCrlf (rest <> more)
                | otherwise -> throwIO $ userError "malformed chunked upload body"

chunkHexSize :: ByteString -> Int
chunkHexSize = BS.foldl' step 0
  where
    step acc c
        | 48 <= c && c <= 57 = acc * 16 + fromIntegral (c - 48)
        | 97 <= c && c <= 102 = acc * 16 + fromIntegral (c - 87)
        | 65 <= c && c <= 70 = acc * 16 + fromIntegral (c - 55)
        | otherwise = acc

assertUploadHeaders :: ByteString -> IO ()
assertUploadHeaders headerBlock = do
    let normalized = BSC.map toLower headerBlock
    unless ("POST" `BSC.isInfixOf` headerBlock) $
        throwIO $
            userError "upload server expected a POST request"
    unless ("transfer-encoding: chunked" `BSC.isInfixOf` normalized) $
        throwIO $
            userError "upload server expected chunked transfer-encoding"
    when ("expect:" `BSC.isInfixOf` normalized) $
        throwIO $
            userError "upload server did not expect a 100-continue handshake"

echoUploadResponder :: Socket -> IO ()
echoUploadResponder = echoUploadResponderWithHeaders $ const $ pure ()

echoUploadResponderWithHeaders :: (ByteString -> IO ()) -> Socket -> IO ()
echoUploadResponderWithHeaders inspectHeaders sock = do
    (headerBlock, remainder) <- receiveRequestHeaders sock
    assertUploadHeaders headerBlock
    inspectHeaders headerBlock
    body <- readChunkedUploadBody sock remainder
    sendFixedResponse "200 OK" [] body sock

withUploadServer :: (Socket -> IO ()) -> (ByteString -> IO a) -> IO a
withUploadServer responder action = bracket openServer close \listener -> do
    port <- socketPort listener
    server <- async do
        (connection, _) <- accept listener
        finally (responder connection) (close connection)
    let url = BSC.pack $ "http://127.0.0.1:" <> show port <> "/"
    action url `finally` stopServer server

uploadRequestFor :: ByteString -> Request
uploadRequestFor url =
    (requestFor url)
        { method = Post
        }

chunkBy :: [Int] -> ByteString -> [ByteString]
chunkBy = go
  where
    go [] rest
        | BS.null rest = []
        | otherwise = [rest]
    go (n : ns) rest
        | BS.null rest = []
        | otherwise =
            let (prefix, suffix) = BS.splitAt n rest
             in (if BS.null prefix then [] else [prefix]) <> go ns suffix

assertUploadOk :: String -> ByteString -> Either CurlCode (Response BSL.ByteString) -> IO ()
assertUploadOk context expectedBody = \case
    Left curlCode -> throwIO $ userError (context <> ": unexpected curl error: " <> show curlCode)
    Right Response{info = HttpParts{statusCode}, body, metrics} -> do
        assertEqual (context <> " status") 200 statusCode
        assertEqual
            (context <> " uploaded bytes")
            (fromIntegral $ BS.length expectedBody)
            metrics.uploadProgress
        let actual = BSL.toStrict body
        unless (actual == expectedBody) $
            throwIO . userError $
                context
                    <> ": echoed body mismatch: expected length "
                    <> show (BS.length expectedBody)
                    <> ", got "
                    <> show (BS.length actual)
                    <> ", first difference at "
                    <> show (firstDifference expectedBody actual)

firstDifference :: ByteString -> ByteString -> Maybe Int
firstDifference first second = go 0
  where
    go index
        | index >= BS.length first || index >= BS.length second =
            if BS.length first == BS.length second
                then Nothing
                else Just $ min (BS.length first) (BS.length second)
        | BS.index first index == BS.index second index = go (index + 1)
        | otherwise = Just index

{- | A POST body of unknown length is chunked by libcurl over HTTP/1.1 and
must arrive byte-for-byte, including chunks larger than curl's read buffer
(the oversized tail is delivered over several callback calls).
-}
testUploadBody :: Agent -> IO ()
testUploadBody agent = do
    let payload = BS.pack $ take 700_000 $ cycle [97 .. 122]
        chunks =
            chunkBy
                [300_000, 17, 4_096, 1, 65_536, 128, 9_000]
                payload
    withUploadServer echoUploadResponder \url ->
        runResourceT do
            result <-
                httpUpload agent (uploadRequestFor url) \body ->
                    do
                        forM_ chunks \chunk -> liftIO do
                            outcome <- feedBody body chunk
                            case outcome of
                                Left curlCode -> throwIO $ userError $ "feedBody: " <> show curlCode
                                Right () -> pure ()
                        liftIO do
                            outcome <- endBody body
                            case outcome of
                                Left curlCode -> throwIO $ userError $ "endBody: " <> show curlCode
                                Right () -> pure ()
            liftIO $ assertUploadOk "upload chunked" payload result

{- | The upload-only Expect suppression is an owned prefix over a borrowed
reusable slist. It must preserve the caller's headers without copying or
mutating that list, and keep the borrowed tail alive until completion.
-}
testUploadOverrideHeaders :: Agent -> IO ()
testUploadOverrideHeaders agent = do
    reusable <- toHeaderSlist ["X-HCurl-Upload: retained"]
    let payload = "request-local-overlay"
        inspectHeaders headerBlock =
            unless ("x-hcurl-upload: retained" `BSC.isInfixOf` BSC.map toLower headerBlock) $
                throwIO $
                    userError "upload server did not receive the reusable header"
    withUploadServer (echoUploadResponderWithHeaders inspectHeaders) \url ->
        runResourceT do
            result <-
                httpUpload
                    agent
                    (uploadRequestFor url){Request.headers = OverrideHeaders reusable}
                    \body -> liftIO do
                        performMajorGC
                        feedBody body payload >>= assertEqual "overlay upload feed" (Right ())
            liftIO $ assertUploadOk "upload reusable headers" payload result

{- | A one-chunk upload queue plus a producer that paces itself forces the
pause/resume handshake on the upload side (curl asks for data, finds the
queue empty, pauses; the next producer write resumes it).
-}
testUploadBackpressure :: Agent -> IO ()
testUploadBackpressure agent = do
    let payload = BS.pack $ take 1_500_000 $ cycle [97, 98, 99]
        chunks = chunkBy (replicate 80 16_384) payload
    withUploadServer echoUploadResponder \url ->
        runResourceT do
            result <-
                httpUploadWith
                    UploadConfig{uploadChunks = 1}
                    agent
                    (uploadRequestFor url)
                    \body ->
                        forM_ chunks \chunk -> liftIO do
                            threadDelay 15_000
                            outcome <- feedBody body chunk
                            case outcome of
                                Left curlCode -> throwIO $ userError $ "feedBody: " <> show curlCode
                                Right () -> pure ()
            liftIO $ assertUploadOk "upload backpressure" payload result

{- | A producer that abandons the body halfway must fail the transfer with a
deterministic 'AbortedByCallback' and leave the agent usable.
-}
testUploadAbort :: Agent -> IO ()
testUploadAbort agent =
    withUploadServer readThenClose \url ->
        runResourceT do
            result <-
                httpUpload agent (uploadRequestFor url) \body -> do
                    forM_ [1 .. 8] \_ ->
                        liftIO $ void $ feedBody body (BS.replicate 16_384 97)
                    liftIO $ abortBody body
            liftIO $ case result of
                Left AbortedByCallback -> pure ()
                Left curlCode -> throwIO $ userError $ "expected AbortedByCallback, got " <> show curlCode
                Right _ -> throwIO $ userError "aborted upload unexpectedly succeeded"
            `finally` withServer (sendFixedResponse "200 OK" [] "still-alive") \aliveUrl ->
                runResourceT (httpLBS agent (requestFor aliveUrl)) >>= assertBufferedOk "after upload abort" "still-alive"
  where
    readThenClose sock = do
        (_, remainder) <- receiveRequestHeaders sock
        _ <- try @SomeException $ readChunkedUploadBody sock remainder
        pure ()

testUploadRepeatedEnd :: Agent -> IO ()
testUploadRepeatedEnd agent = do
    responseSent <- newEmptyMVar
    withUploadServer (fastCompletionResponder responseSent) \url ->
        runResourceT do
            result <-
                httpUpload agent (uploadRequestFor url) \body -> liftIO do
                    endBody body >>= assertEqual "explicit upload EOF" (Right ())
                    feedBody body BS.empty
                        >>= assertEqual "empty upload chunk after EOF" (Left AbortedByCallback)
                    takeMVar responseSent
                    -- Let the agent observe completion before httpUpload performs
                    -- its own idempotent EOF operation on callback return.
                    threadDelay 100_000
            liftIO $ assertUploadOk "repeated upload EOF" BS.empty result
  where
    fastCompletionResponder responseSent sock = do
        (headerBlock, remainder) <- receiveRequestHeaders sock
        assertUploadHeaders headerBlock
        requestBody <- readChunkedUploadBody sock remainder
        assertEqual "fast completion request body" BS.empty requestBody
        sendFixedResponse "200 OK" [] BS.empty sock
        putMVar responseSent ()

testUploadRedirects :: Agent -> IO ()
testUploadRedirects agent =
    forM_ [("307 Temporary Redirect", 307), ("308 Permanent Redirect", 308)] $ \(status, expectedCode) ->
        withServer (sendFixedResponse "200 OK" [] "incorrectly-followed") \finalUrl ->
            withUploadServer (uploadRedirectResponder status finalUrl) \redirectUrl ->
                runResourceT do
                    result <-
                        httpUpload agent (uploadRequestFor redirectUrl) \body -> liftIO do
                            feedBody body "stream-once" >>= assertEqual "redirect upload feed" (Right ())
                    liftIO $ case result of
                        Left curlCode -> throwIO $ userError $ "streaming redirect failed: " <> show curlCode
                        Right Response{info = HttpParts{statusCode}, body} -> do
                            assertEqual "streaming redirect status" expectedCode statusCode
                            assertEqual "streaming redirect body" "not-followed" body
  where
    uploadRedirectResponder status finalUrl sock = do
        (headerBlock, remainder) <- receiveRequestHeaders sock
        assertUploadHeaders headerBlock
        requestBody <- readChunkedUploadBody sock remainder
        assertEqual "redirect upload body" "stream-once" requestBody
        sendFixedResponse status [("Location", finalUrl)] "not-followed" sock

testUploadZeroChunks :: Agent -> IO ()
testUploadZeroChunks agent = do
    outcome <-
        try @InvalidStreamBufferSize $
            withUploadServer echoUploadResponder \url ->
                runResourceT $
                    httpUploadWith UploadConfig{uploadChunks = 0} agent (uploadRequestFor url) \_ -> pure ()
    case outcome of
        Left InvalidStreamBufferSize -> pure ()
        Right _ -> throwIO $ userError "expected InvalidStreamBufferSize for zero upload chunks"

testUploadValidation :: Agent -> IO ()
testUploadValidation agent = do
    let badMethod = (requestFor "http://127.0.0.1:1/"){method = Get}
        badBody = (requestFor "http://127.0.0.1:1/"){method = Post, body = Buffer "fixed"}
        badRedirect =
            (requestFor "http://127.0.0.1:1/")
                { method = Post
                , extraOptions = [OptionFollowLocation True]
                }
    forM_
        [("non-POST", badMethod), ("pre-set body", badBody), ("redirect replay", badRedirect)]
        \(label, badRequest) -> do
            outcome <-
                try @InvalidRequest $
                    runResourceT $
                        httpUpload agent badRequest \_ -> pure ()
            case outcome of
                Left _ -> pure ()
                Right _ -> throwIO $ userError $ label <> ": expected a validation error"

testUploadUnsupported :: Agent -> IO ()
testUploadUnsupported agent = do
    let request = (requestFor "http://127.0.0.1:1/"){method = Post}
    try @StreamingUploadUnsupported (runResourceT $ httpUpload agent request $ const $ pure ()) >>= \case
        Left StreamingUploadUnsupported -> pure ()
        Right _ -> throwIO $ userError "curl 7.18 unexpectedly enabled streaming upload"

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
