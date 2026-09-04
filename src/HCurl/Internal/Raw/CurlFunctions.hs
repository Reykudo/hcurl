{-# LANGUAGE CApiFFI #-}

module HCurl.Internal.Raw.CurlFunctions where

import Foreign
import Foreign.C
import HCurl.Internal.Raw.Curl

foreign import ccall unsafe "options.h hcurl_raw_easy_setopt_long"
    curl_easy_setopt_long :: Ptr CurlEasy -> CInt -> CLong -> IO CInt

foreign import ccall unsafe "options.h hcurl_raw_easy_setopt_string"
    curl_easy_setopt_string :: Ptr CurlEasy -> CInt -> Ptr CChar -> IO CInt

foreign import ccall unsafe "options.h hcurl_easy_setopt_long"
    hcurl_easy_setopt_long :: Ptr CurlEasy -> CInt -> CLong -> IO CInt

foreign import ccall unsafe "options.h hcurl_easy_setopt_string"
    hcurl_easy_setopt_string :: Ptr CurlEasy -> CInt -> Ptr CChar -> IO CInt
