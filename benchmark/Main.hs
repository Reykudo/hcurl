{-# OPTIONS_GHC -Wno-orphans #-}

module Main where

import Control.Concurrent.Async (forConcurrently)
import Control.DeepSeq
import Control.Monad.Trans.Resource (runResourceT)
import Criterion.Main as Criterion
import Criterion.Types (Config (verbosity), Verbosity (..))
import Data.Atomics.Counter
import Data.ByteString
import Data.ByteString.Lazy qualified as LBS
import HCurl.Agent as Agent
import HCurl.Request as Request
import HCurl.Response qualified as HCurl
import HCurl.Simple
import HCurl.Types
import Network.HTTP.Client (Response (..))
import Network.HTTP.Client qualified as HTTPClient
import Network.HTTP.Simple qualified as HTTPSimple
import UnliftIO.Exception (try)

instance NFData HTTPClient.HttpException where
    rnf !ex = case ex of
        HTTPClient.HttpExceptionRequest _ !_ -> ()
        HTTPClient.InvalidUrlException _ _ -> ()

main :: IO ()
main = do
    initCurl
    let conf =
            AgentConfig
                { connectionCacheSize = 5000
                , maxConnectionPerHost = 100
                , maxConnection = 1000
                }
    Agent.withAgent conf \agent -> do
        hcurlCounterSuccess <- newCounter 0
        hcurlCounterFailure <- newCounter 0
        httpCounterSuccess <- newCounter 0
        httpCounterFailure <- newCounter 0
        let criterionConfig =
                Criterion.defaultConfig
                    { verbosity = Verbose
                    }
        defaultMainWith
            criterionConfig
            [ bgroup
                "hcurl"
                [ bench "1" $ nfIO (makeGetRequestHCurl hcurlCounterSuccess hcurlCounterFailure agent)
                , bench "10" $ nfIO (makeMultpleParallel 10 (makeGetRequestHCurl hcurlCounterSuccess hcurlCounterFailure agent))
                , bench "100" $ nfIO (makeMultpleParallel 100 (makeGetRequestHCurl hcurlCounterSuccess hcurlCounterFailure agent))
                , bench "1000" $ nfIO (makeMultpleParallel 1000 (makeGetRequestHCurl hcurlCounterSuccess hcurlCounterFailure agent))
                ]
            , bgroup
                "http-client"
                [ bench "1" $ nfIO (makeGetRequestHTTPClient httpCounterSuccess httpCounterFailure)
                , bench "10" $ nfIO (makeMultpleParallel 10 (makeGetRequestHTTPClient httpCounterSuccess httpCounterFailure))
                , bench "100" $ nfIO (makeMultpleParallel 100 (makeGetRequestHTTPClient httpCounterSuccess httpCounterFailure))
                ]
            ]
        hcurlCounterSuccessVal <- readCounter hcurlCounterSuccess
        hcurlCounterFailureVal <- readCounter hcurlCounterFailure
        httpCounterSuccessVal <- readCounter httpCounterSuccess
        httpCounterFailureVal <- readCounter httpCounterFailure
        putStrLn $
            "hcurl counter - success "
                <> show hcurlCounterSuccessVal
                <> " - failed "
                <> show hcurlCounterFailureVal
        putStrLn $
            "http counter - success "
                <> show httpCounterSuccessVal
                <> " - failed "
                <> show httpCounterFailureVal

makeMultpleParallel :: Int -> IO a -> IO [a]
makeMultpleParallel n action =
    forConcurrently [1 .. n] $ const action

makeGetRequestHCurl :: AtomicCounter -> AtomicCounter -> Agent -> IO (Either CurlCode (HCurl.Response LBS.ByteString))
makeGetRequestHCurl sCounter fCounter agent = do
    !res <- runResourceT $ httpLBS agent hcurlGetRequest
    case res of
        Left _ -> incrCounter_ 1 fCounter
        Right _ -> incrCounter_ 1 sCounter
    pure $! res

hcurlGetRequest :: Request
hcurlGetRequest =
    (defaultRequest "https://example.com/")
        { connectionTimeoutMS = 400
        , lowSpeedLimit = LowSpeedLimit{timeout = 1, lowSpeed = 1}
        }

makeGetRequestHTTPClient :: AtomicCounter -> AtomicCounter -> IO (Either HTTPSimple.HttpException ByteString)
makeGetRequestHTTPClient sCounter fCounter = do
    !res <- try @_ @HTTPSimple.HttpException $! HTTPSimple.httpBS httpClientGetRequest
    case res of
        Left _ -> incrCounter_ 1 fCounter
        Right _ -> incrCounter_ 1 sCounter
    pure $! (\(!x) -> x.responseBody) <$> res

httpClientGetRequest :: HTTPSimple.Request
httpClientGetRequest =
    req'
        { HTTPClient.responseTimeout = HTTPClient.responseTimeoutMicro 400000
        }
  where
    req' = HTTPSimple.parseRequest_ "https://example.com/"
