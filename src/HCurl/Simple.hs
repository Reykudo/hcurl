{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Simple where

import Data.ByteString.Lazy qualified as BSL
import Language.C.Inline qualified as C

import HCurl.Internal.Raw

import Data.Foldable (traverse_)
import HCurl.Agent
import HCurl.Extras
import HCurl.Internal.Easy
import HCurl.Internal.Metrics
import HCurl.Internal.Raw.Extras (getCurlCode)
import HCurl.Internal.Raw.MPSC (OuterMessage (Execute))
import HCurl.Internal.Raw.SimpleString (SimpleStringPtr)
import HCurl.Internal.Response (getHttpParts)
import HCurl.Request
import HCurl.Response
import UnliftIO
import UnliftIO.Resource

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<string.h>"
C.include "<stdlib.h>"

C.include "<curl/curl.h>"
C.include "HsFFI.h"

initCurl :: IO ()
initCurl = [C.block|void { curl_global_init(CURL_GLOBAL_DEFAULT); }|]

performRequest :: AgentHandle -> RequestHandler SimpleStringPtr -> IO (Either CurlCode (Response BSL.ByteString))
performRequest agent reqHandler = do
    let CurlEasy easyPtr = reqHandler.easy
    sendMessage agent.agentContext $ Execute easyPtr
    readMVar reqHandler.doneRequest
    !responseBS <- simpleStringToBS reqHandler.responseTarget
    getCurlCode reqHandler.easyData >>= \case
        Ok -> do
            metrics <- extractMetrics reqHandler.metricsContext
            info <- getHttpParts reqHandler.easy reqHandler.requestHeaders
            pure . Right $!
                Response
                    { info
                    , body = BSL.fromStrict responseBS
                    , metrics
                    }
        err -> pure $ Left err

httpLBS :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> m (Either CurlCode (Response BSL.ByteString))
httpLBS agent request = do
    lease <- liftIO $ acquireLease agent
    outcome <-
        ( do
            (releaseKeyEasy, easy) <- allocateEasy
            req <- initRequest request easy
            res <- liftIO $ performRequest lease.leaseAgentHandle req `onException` cancelRequest lease.leaseAgentHandle.agentContext req
            traverse_ release req.resources
            release releaseKeyEasy
            pure res
        )
            `finally` liftIO lease.leaseDone
    pure outcome
