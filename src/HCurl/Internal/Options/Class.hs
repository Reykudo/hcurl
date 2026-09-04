{-# LANGUAGE AllowAmbiguousTypes #-}

module HCurl.Internal.Options.Class where

import Data.Kind
import Foreign.Ptr
import HCurl.Internal.Raw.Curl

class EasyOption option where
    type CurlParamBaseType option :: Type
    setEasyOption :: Ptr CurlEasy -> CurlParamBaseType option -> IO ()
