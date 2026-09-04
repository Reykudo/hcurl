module HCurl.Options (
    HTTPVersion (..),
    IPResolve (..),
    InvalidOptionValue (..),
    SomeOption (..),
) where

import Control.DeepSeq (NFData)
import Control.Exception (Exception)
import Data.ByteString (ByteString)
import GHC.Generics (Generic)
import HCurl.Internal.Raw.Curl (HTTPVersion (..))

-- | Version-stable options supported by the high-level request interface.
data SomeOption
    = OptionHttpVersion !HTTPVersion
    | OptionPipeWait !Bool
    | OptionFollowLocation !Bool
    | OptionNoSignal !Bool
    | OptionSslVerifyPeer !Bool
    | OptionTcpFastOpen !Bool
    | OptionTcpKeepAlive !Bool
    | OptionTimeoutMs !Int
    | OptionConnectTimeoutMs !Int
    | OptionLowSpeedTime !Int
    | OptionLowSpeedLimit !Int
    | OptionAcceptEncoding !ByteString
    | OptionIpResolve !IPResolve
    | OptionSslVerifyHost !Bool
    deriving (Show, Eq, Generic)
    deriving anyclass (NFData)

data IPResolve = ResolveWhatever | ResolveIPv4 | ResolveIPv6
    deriving (Show, Eq, Ord, Enum, Bounded, Generic)
    deriving anyclass (NFData)

data InvalidOptionValue
    = OptionStringContainsNul !String
    | OptionStringContainsNewline !String
    | OptionIntegerIsNegative !String !Integer
    | OptionIntegerOutOfRange !String !Integer
    deriving (Show)

instance Exception InvalidOptionValue
