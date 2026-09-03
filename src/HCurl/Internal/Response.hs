{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Response (getHttpParts) where

import HCurl.Internal.Headers (extractHeaders)
import HCurl.Internal.Raw
import HCurl.Internal.Raw.Headers (HeadersData)
import HCurl.Response (HttpParts (..))
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<curl/curl.h>"

getHttpParts :: CurlEasy -> HeadersData -> IO HttpParts
getHttpParts (CurlEasy easyPtr) headerData = do
    statusCode <-
        fromIntegral
            <$> [CU.block|long {
                long http_code = 0;
                curl_easy_getinfo($(CURL* easyPtr), CURLINFO_RESPONSE_CODE, &http_code);
                return http_code;
            }|]
    headers <- extractHeaders headerData
    pure HttpParts{statusCode, headers}
