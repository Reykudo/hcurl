{-# LANGUAGE ImpredicativeTypes #-}

module HCurl.Request (
    InvalidRequest (..),
    LowSpeedLimit (..),
    Request (..),
    RequestHeader (..),
    defaultRequest,
) where

import Control.DeepSeq
import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Word (Word8)
import GHC.Generics
import HCurl.Internal.Raw
import HCurl.Options
import HCurl.Types

data InvalidRequest
    = RequestURLIsEmpty
    | RequestContainsNul !String
    | RequestContainsControl !String !Word8
    | NegativeRequestTimeout !String !Int
    | HeadRequestHasBody
    | StreamingUploadRequiresPost
    | StreamingUploadRequiresEmptyBody
    | StreamingUploadRedirectsUnsupported
    | RequestValueOutOfRange !String !Integer
    | InvalidHTTPMethod !BS.ByteString
    | RequestHeaderContainsNul !Int
    | RequestHeaderContainsNewline !Int
    deriving (Show)

instance Exception InvalidRequest

data Request = Request
    { url :: !ByteString
    , timeoutMS :: !Int
    , connectionTimeoutMS :: !Int
    , lowSpeedLimit :: !LowSpeedLimit
    , body :: !Body
    , method :: !HTTPMethod
    , headers :: !RequestHeader
    , extraOptions :: ![SomeOption]
    }
    deriving (Generic)
    deriving anyclass (NFData)

data RequestHeader = NoHeaders | HeaderList ![BS.ByteString] | OverrideHeaders !CurlSlist
    deriving (Generic)
    deriving anyclass (NFData)

data LowSpeedLimit = LowSpeedLimit
    { lowSpeed :: !Int
    , timeout :: !Int
    }
    deriving (Generic)
    deriving anyclass (NFData)

-- | A GET request with libcurl's timeout defaults and no custom headers.
defaultRequest :: ByteString -> Request
defaultRequest requestUrl =
    Request
        { url = requestUrl
        , timeoutMS = 0
        , connectionTimeoutMS = 0
        , lowSpeedLimit = LowSpeedLimit{lowSpeed = 0, timeout = 0}
        , body = Empty
        , method = Get
        , headers = NoHeaders
        , extraOptions = []
        }
