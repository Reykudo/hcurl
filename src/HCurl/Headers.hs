module HCurl.Headers (
    CurlSlist,
    CurlSlistError (..),
    toHeaderSlist,
) where

import HCurl.Internal.Raw.Curl (CurlSlist)
import HCurl.Internal.Slist (CurlSlistError (..), toHeaderSlist)
