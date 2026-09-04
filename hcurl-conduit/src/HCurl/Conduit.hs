{-# LANGUAGE LambdaCase #-}

module HCurl.Conduit (
    bodyReaderSource,
    http,
    httpWith,
)
where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Resource (MonadResource)
import Data.ByteString (ByteString)
import Data.Conduit (ConduitT, bracketP, yield)
import HCurl.Agent (Agent)
import HCurl.Request (Request)
import HCurl.Response (StreamingResponse (..))
import HCurl.Streaming (
    BodyReader,
    StreamConfig,
    closeBody,
    defaultStreamConfig,
    httpStreamingWith,
    readBody,
 )
import HCurl.Types (CurlCode)
import UnliftIO (MonadUnliftIO, throwIO)

{- | Turn hcurl's framework-independent 'BodyReader' into a Conduit source.
The transfer is closed when downstream stops early as well as at EOF.
-}
bodyReaderSource :: (MonadResource m, MonadIO m) => BodyReader -> ConduitT i ByteString m ()
bodyReaderSource reader = bracketP (pure reader) closeBody stream
  where
    stream bodyReader = go
      where
        go =
            liftIO (readBody bodyReader) >>= \case
                Left curlCode -> liftIO $ throwIO curlCode
                Right Nothing -> pure ()
                Right (Just chunk) -> yield chunk >> go

http :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> m (Either CurlCode (StreamingResponse (ConduitT () ByteString m ())))
http = httpWith defaultStreamConfig

httpWith :: (MonadResource m, MonadUnliftIO m) => StreamConfig -> Agent -> Request -> m (Either CurlCode (StreamingResponse (ConduitT () ByteString m ())))
httpWith streamConfig agent request =
    fmap toConduitResponse <$> httpStreamingWith streamConfig agent request

toConduitResponse :: (MonadResource m, MonadIO m) => StreamingResponse BodyReader -> StreamingResponse (ConduitT () ByteString m ())
toConduitResponse StreamingResponse{info = responseInfo, body = responseBody, completion = responseCompletion} =
    StreamingResponse
        { info = responseInfo
        , body = bodyReaderSource responseBody
        , completion = responseCompletion
        }
