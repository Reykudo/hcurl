{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE PartialTypeSignatures #-}

{- | Streaming request bodies (upload), mirroring the response streaming in
"HCurl.Streaming".  Start a POST whose body is fed from Haskell through an
@UploadBody@ writer; libcurl drains the bounded queue from its read callback
and applies backpressure exactly like the download side does:

> httpUpload agent request \body -> do
>     feedBody body chunk1
>     feedBody body chunk2
>     endBody body

'feedBody' blocks while the queue is full, so a slow server throttles the
producer; 'endBody' closes the body (EOF), after which the response is
returned normally.  The body length is unknown to libcurl, so HTTP/1.1
transfers use chunked transfer-encoding automatically.
-}
module HCurl.Upload (
    UploadBody,
    UploadConfig (..),
    InvalidStreamBufferSize (..),
    StreamingUploadUnsupported (..),
    abortBody,
    defaultUploadConfig,
    endBody,
    feedBody,
    httpUpload,
    httpUploadWith,
    streamingUploadSupported,
)
where

import Control.Exception (SomeException)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BSL
import HCurl.Agent (Agent)
import HCurl.Internal.Body
import HCurl.Internal.Easy
import HCurl.Internal.Once (newOnce)
import HCurl.Internal.Response (getBufferedResponse)
import HCurl.Internal.Transfer
import HCurl.Request (Request)
import HCurl.Response (Response (..))
import HCurl.Types (CurlCode)
import Numeric.Natural (Natural)
import UnliftIO (MonadUnliftIO, liftIO, mask, mask_, onException, throwIO, try, withRunInIO)
import UnliftIO.Resource

newtype UploadConfig = UploadConfig
    { uploadChunks :: Natural
    }
    deriving (Show, Eq)

defaultUploadConfig :: UploadConfig
defaultUploadConfig = UploadConfig{uploadChunks = 16}

{- | Producer handle for a streaming request body.  Feed it from the callback
passed to 'httpUpload' (or from any thread the callback shares it with).
-}
data UploadBody = UploadBody !UploadStreamState !(IO ())

{- | Enqueue one request-body chunk.  Blocks while the bounded upload queue
is full, i.e. while libcurl is still sending earlier data (backpressure).
Returns 'Left' with the transfer's 'CurlCode' when the request already ended
or the body was closed.
-}
feedBody :: UploadBody -> ByteString -> IO (Either CurlCode ())
feedBody (UploadBody state _) = writeUploadChunk state

{- | Signal the end of the request body (EOF).  After this the server sees
the terminating chunk and the request completes normally.  Idempotent.
-}
endBody :: UploadBody -> IO (Either CurlCode ())
endBody (UploadBody state _) = finishUploadBody state

{- | Abandon the rest of the request body.  Subsequent 'feedBody' calls
return 'Left' and the transfer fails deterministically with
'CurlCode' @AbortedByCallback@.
-}
abortBody :: UploadBody -> IO ()
abortBody (UploadBody _ closeUpload) = closeUpload

httpUpload :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> (UploadBody -> m ()) -> m (Either CurlCode (Response BSL.ByteString))
httpUpload = httpUploadWith defaultUploadConfig

{- | 'httpUpload' with an explicit @UploadConfig@ (bounded queue length).
The request must use @Post@ and carry an @Empty@ body: the body is fed
exclusively through the writer. Streaming upload requires libcurl 7.19 or
newer. The rest of the package supports libcurl 7.18.x, but its unsafe
@CURL_READFUNC_PAUSE@ implementation is rejected with
@StreamingUploadUnsupported@.
-}
httpUploadWith :: (MonadResource m, MonadUnliftIO m) => UploadConfig -> Agent -> Request -> (UploadBody -> m ()) -> m (Either CurlCode (Response BSL.ByteString))
httpUploadWith config agent request feed = mask \restore -> do
    liftIO $ validateStreamBufferSize config.uploadChunks
    transfer <-
        startTransferWith UseStreamingRequestBody agent request \context identifier easy _headers metrics -> do
            responseTarget <- setSimpleStringResponse easy
            uploadState <- liftIO $ newUploadBodyState (uploadChunks config) context identifier
            resources <- installUploadStream easy metrics uploadState
            pure ((responseTarget, uploadState), resources, uploadTransferStreams uploadState)
    let requestHandler = transfer.transferHandler
        (responseTarget, uploadState) = requestHandler.responseTarget
    closeUpload <- liftIO $ newOnce $ markUploadClosed uploadState
    let uploadBody = UploadBody uploadState closeUpload
        cancelTransfer = abortBody uploadBody >> transfer.finishTransfer
    cleanupKey <- register cancelTransfer `onException` liftIO cancelTransfer
    unregisterCleanup <- withRunInIO \runInIO ->
        pure $ void $ runInIO $ unprotect cleanupKey
    let finishUpload = mask_ $ transfer.finishTransfer >> unregisterCleanup
        cleanupUpload = mask_ $ cancelTransfer >> unregisterCleanup

    feedResult <- tryAny $ restore $ feed uploadBody
    case feedResult of
        Left exception -> liftIO cleanupUpload >> throwIO exception
        Right () -> do
            endResult <- liftIO $ endBody uploadBody
            case endResult of
                Left curlCode -> liftIO finishUpload >> pure (Left curlCode)
                Right () -> do
                    response <-
                        restore (liftIO $ getBufferedResponse requestHandler responseTarget)
                            `onException` liftIO cleanupUpload
                    liftIO finishUpload
                    pure response

tryAny :: (MonadUnliftIO m) => m a -> m (Either SomeException a)
tryAny = try
