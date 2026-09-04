module Main where

import Control.Monad.Trans.Resource (runResourceT)
import HCurl.Agent as Agent
import HCurl.Request as Request
import HCurl.Simple
import HCurl.Types

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
        resp <- runResourceT $ httpLBS agent hcurlGetRequest
        print resp

hcurlGetRequest :: Request
hcurlGetRequest =
    (defaultRequest "https://example.com/")
        { connectionTimeoutMS = 1000
        , lowSpeedLimit = LowSpeedLimit{timeout = 1, lowSpeed = 1}
        }
