module HCurl.Response where

import Control.DeepSeq
import GHC.Generics
import HCurl.Internal.Metrics (Metrics)
import HCurl.Internal.Raw.Curl (CurlCode)
import Network.HTTP.Types.Header

data Response body = Response
    { info :: !HttpParts
    , body :: !body
    , metrics :: !Metrics
    }
    deriving (Show, Eq, Generic)
    deriving anyclass (NFData)

data HttpParts = HttpParts
    { statusCode :: !Int
    , headers :: !RequestHeaders
    }
    deriving (Show, Eq, Generic)
    deriving anyclass (NFData)

{- | A response whose body can be consumed before the transfer has finished.

'completion' waits for the final transfer result and metrics.  With a
bounded body reader it must be used after, or concurrently with, consuming
'body'; waiting for completion while leaving the body unread can apply
backpressure and intentionally pause libcurl.
-}
data StreamingResponse body = StreamingResponse
    { info :: !HttpParts
    , body :: !body
    , completion :: IO (Either CurlCode Metrics)
    }
