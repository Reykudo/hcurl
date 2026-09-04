module HCurl.Metrics (Metrics (..)) where

import Control.DeepSeq (NFData)
import Data.Int (Int64)
import GHC.Generics (Generic)

{- | Transfer counters and rates are bytes and bytes/second respectively;
timing fields are microseconds. 'Int64' matches libcurl's @curl_off_t@
metrics and does not truncate on 32-bit Haskell runtimes.
-}
data Metrics = Metrics
    { uploadProgress :: !Int64
    , uploadTotal :: !Int64
    , downloadProgress :: !Int64
    , downloadTotal :: !Int64
    , uploadSpeed :: !Int64
    , downloadSpeed :: !Int64
    , namelookupTime :: !Int64
    , connectTime :: !Int64
    , appconnectTime :: !Int64
    , pretransferTime :: !Int64
    , starttransferTime :: !Int64
    , totalTime :: !Int64
    , redirectTime :: !Int64
    }
    deriving (Show, Eq, Generic)
    deriving anyclass (NFData)
