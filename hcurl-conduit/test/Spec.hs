{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Async, async, cancel, waitCatch)
import Control.Concurrent.MVar
import Control.Exception (SomeException, bracket, finally, throwIO, try)
import Control.Monad (unless, void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.Conduit
import HCurl.Agent
import HCurl.Conduit qualified as Conduit
import HCurl.Internal.Metrics (Metrics (..))
import HCurl.Internal.Raw (CurlCode (..))
import HCurl.Request
import HCurl.Response
import HCurl.Simple (httpLBS, initCurl)
import HCurl.Streaming
import HCurl.Types
import Network.Socket
import Network.Socket.ByteString qualified as Socket
import System.Timeout qualified as Timeout

main :: IO ()
main = withSocketsDo do
    initCurl
    agent <- spawnAgent defaultConfig
    runTest "buffered response remains compatible" $ testBuffered agent
    runTest "core exposes the first chunk before EOF" $ testEarlyCoreStreaming agent
    runTest "bounded core reader pauses and resumes libcurl" $ testBackpressure agent
    runTest "empty response completes without a body callback" $ testEmptyResponse agent
    runTest "truncated response reports a late curl error" $ testLateFailure agent
    runTest "Conduit adapter streams the whole response" $ testConduit agent
    runTest "Conduit early termination cancels the transfer" $ testConduitCancellation agent

runTest :: String -> IO () -> IO ()
runTest name action = do
    putStrLn $ "[ RUN      ] " <> name
    outcome <- try @SomeException $ within 5_000_000 action
    case outcome of
        Left exception -> do
            putStrLn $ "[  FAILED  ] " <> name
            throwIO exception
        Right () -> putStrLn $ "[       OK ] " <> name

within :: Int -> IO a -> IO a
within microseconds action = do
    result <- Timeout.timeout microseconds action
    maybe (throwIO $ userError "test timed out") pure result

assertEqual :: (Eq a, Show a) => String -> a -> a -> IO ()
assertEqual label expected actual =
    unless (expected == actual) . throwIO . userError $
        label <> ": expected " <> show expected <> ", got " <> show actual

assertBool :: String -> Bool -> IO ()
assertBool label condition = unless condition . throwIO $ userError label

requestFor :: ByteString -> Request
requestFor url =
    Request
        { host = url
        , timeoutMS = 4_000
        , connectionTimeoutMS = 1_000
        , lowSpeedLimit = LowSpeedLimit{timeout = 0, lowSpeed = 0}
        , body = Empty
        , method = Get
        , headers = NoHeaders
        , extraOptions = []
        }

testBuffered :: Agent -> IO ()
testBuffered agent =
    withServer
        (sendFixedResponse "201 Created" [("X-HCurl-Test", "buffered")] "buffered-body")
        \url -> do
            result <- runResourceT $ httpLBS agent (requestFor url)
            case result of
                Left curlCode -> throwIO $ userError ("unexpected curl error: " <> show curlCode)
                Right Response{info = HttpParts{statusCode, headers}, body} -> do
                    assertEqual "status" 201 statusCode
                    assertEqual "body" "buffered-body" body
                    assertBool "response header was not parsed" $ ("X-HCurl-Test", "buffered") `elem` headers

testEarlyCoreStreaming :: Agent -> IO ()
testEarlyCoreStreaming agent = do
    releaseTail <- newEmptyMVar
    withServer
        ( \socket -> do
            sendHeaders socket "200 OK" 10 []
            Socket.sendAll socket "first"
            takeMVar releaseTail
            Socket.sendAll socket "-tail"
        )
        \url ->
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("unexpected curl error: " <> show curlCode)
                    Right StreamingResponse{info = HttpParts{statusCode}, body = reader, completion} -> liftIO do
                        assertEqual "status" 200 statusCode
                        first <- readBody reader
                        assertEqual "first read" (Right $ Just "first") first
                        putMVar releaseTail ()
                        (bodyResult, streamResult) <- drainBody reader
                        assertEqual "remaining body" "-tail" bodyResult
                        assertEqual "stream completion" (Right ()) streamResult
                        metricsResult <- completion
                        case metricsResult of
                            Left curlCode -> throwIO $ userError ("unexpected completion error: " <> show curlCode)
                            Right metrics -> assertEqual "download progress" 10 metrics.downloadProgress

testBackpressure :: Agent -> IO ()
testBackpressure agent = do
    let payload = BS.concat $ replicate 128 "0123456789abcdef0123456789abcdef"
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

testEmptyResponse :: Agent -> IO ()
testEmptyResponse agent =
    withServer (sendFixedResponse "204 No Content" [] "") \url ->
        runResourceT do
            result <- httpStreaming agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("unexpected curl error: " <> show curlCode)
                Right StreamingResponse{info = HttpParts{statusCode}, body = reader, completion} -> liftIO do
                    assertEqual "status" 204 statusCode
                    readBody reader >>= assertEqual "empty body" (Right Nothing)
                    completion >>= \case
                        Left curlCode -> throwIO $ userError ("unexpected completion error: " <> show curlCode)
                        Right _ -> pure ()

testLateFailure :: Agent -> IO ()
testLateFailure agent =
    withServer
        ( \socket -> do
            sendHeaders socket "200 OK" 10 []
            Socket.sendAll socket "short"
        )
        \url ->
            runResourceT do
                result <- httpStreaming agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("response did not start: " <> show curlCode)
                    Right StreamingResponse{body = reader, completion} -> liftIO do
                        (actual, streamResult) <- drainBody reader
                        assertEqual "partial body" "short" actual
                        assertEqual "late body error" (Left PartialFile) streamResult
                        completion >>= assertEqual "late completion error" (Left PartialFile)

testConduit :: Agent -> IO ()
testConduit agent =
    withServer (sendFixedResponse "200 OK" [] "conduit-body") \url ->
        runResourceT do
            result <- Conduit.http agent (requestFor url)
            case result of
                Left curlCode -> liftIO . throwIO $ userError ("unexpected curl error: " <> show curlCode)
                Right StreamingResponse{body = source, completion} -> do
                    actual <- runConduit $ source .| sinkByteString
                    liftIO $ assertEqual "conduit body" "conduit-body" actual
                    liftIO completion >>= \case
                        Left curlCode -> liftIO . throwIO $ userError ("unexpected completion error: " <> show curlCode)
                        Right _ -> pure ()

testConduitCancellation :: Agent -> IO ()
testConduitCancellation agent =
    withServer
        ( \socket -> do
            sendHeaders socket "200 OK" 1_000_000 []
            Socket.sendAll socket "first"
            let loop = threadDelay 20_000 >> Socket.sendAll socket "more" >> loop
            void (try @SomeException loop)
        )
        \url ->
            runResourceT do
                result <- Conduit.httpWith StreamConfig{bufferedChunks = 1} agent (requestFor url)
                case result of
                    Left curlCode -> liftIO . throwIO $ userError ("unexpected curl error: " <> show curlCode)
                    Right StreamingResponse{body = source, completion} -> do
                        -- runConduitRes gives the source its own resource scope,
                        -- so leaving the pipeline after the first chunk closes the
                        -- body promptly and aborts the underlying transfer.
                        first <- liftIO $ runConduitRes $ source .| await
                        liftIO $ assertEqual "first conduit chunk" (Just "first") first
                        liftIO (completion >>= assertEqual "cancel result" (Left AbortedByCallback))

drainBody :: BodyReader -> IO (ByteString, Either CurlCode ())
drainBody reader = go []
  where
    go chunks =
        readBody reader >>= \case
            Left curlCode -> pure (BS.concat $ reverse chunks, Left curlCode)
            Right Nothing -> pure (BS.concat $ reverse chunks, Right ())
            Right (Just chunk) -> go (chunk : chunks)

sinkByteString :: (Monad m) => ConduitT ByteString o m ByteString
sinkByteString = go []
  where
    go chunks =
        await >>= \case
            Nothing -> pure . BS.concat $ reverse chunks
            Just chunk -> go (chunk : chunks)

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
receiveRequest socket = go ""
  where
    go received
        | "\r\n\r\n" `BS.isInfixOf` received = pure ()
        | otherwise = do
            chunk <- Socket.recv socket 4_096
            if BS.null chunk
                then throwIO $ userError "client closed before sending request headers"
                else go $ received <> chunk

sendFixedResponse :: ByteString -> [(ByteString, ByteString)] -> ByteString -> Socket -> IO ()
sendFixedResponse status extraHeaders responseBody socket = do
    sendHeaders socket status (BS.length responseBody) extraHeaders
    Socket.sendAll socket responseBody

sendHeaders :: Socket -> ByteString -> Int -> [(ByteString, ByteString)] -> IO ()
sendHeaders socket status contentLength extraHeaders =
    Socket.sendAll socket . BS.concat $
        [ "HTTP/1.1 "
        , status
        , "\r\nContent-Length: "
        , BSC.pack $ show contentLength
        , "\r\nConnection: close\r\n"
        , BS.concat $ fmap (\(name, value) -> name <> ": " <> value <> "\r\n") extraHeaders
        , "\r\n"
        ]
