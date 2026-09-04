{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Multi where

import Control.Exception
import Control.Monad (unless)
import Foreign
import Foreign.C.Types
import HCurl.Internal.Raw
import HCurl.Types
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<curl/curl.h>"
C.include "extras.h"

newtype CurlMultiInitFailed = CurlMultiInitFailed Int
    deriving (Show)

instance Exception CurlMultiInitFailed

initCurlGlobal :: IO ()
initCurlGlobal = do
    code <- [CU.exp| int { (int)hcurl_global_init_once() } |]
    unless (code == 0) $ throwIO (toEnum (fromIntegral code) :: CurlCode)

initCurlMulti :: AgentConfig -> IO (Ptr CurlMulti)
initCurlMulti config = do
    initCurlGlobal
    maxTotal <- checkedNatural "maxConnection" config.maxConnection
    maxPerHost <- checkedNatural "maxConnectionPerHost" config.maxConnectionPerHost
    cacheSize <- checkedNatural "connectionCacheSize" config.connectionCacheSize
    alloca \(statusPtr :: Ptr CInt) -> do
        multi <-
            [CU.block| CURLM* {
                CURLMcode status = CURLM_OK;
                CURLM *multi = curl_multi_init();
                if (multi == NULL) {
                    *$(int* statusPtr) = -1;
                    return NULL;
                }
#if LIBCURL_VERSION_NUM >= 0x071e00
                if ($(long maxTotal) > 0) {
                    status = curl_multi_setopt(multi, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                                               $(long maxTotal));
                }
                if (status == CURLM_OK && $(long maxPerHost) > 0) {
                    status = curl_multi_setopt(multi, CURLMOPT_MAX_HOST_CONNECTIONS,
                                               $(long maxPerHost));
                }
#else
                if ($(long maxTotal) > 0 || $(long maxPerHost) > 0) {
                    status = CURLM_UNKNOWN_OPTION;
                }
#endif
                if (status == CURLM_OK && $(long cacheSize) > 0) {
                    status = curl_multi_setopt(multi, CURLMOPT_MAXCONNECTS,
                                               $(long cacheSize));
                }
                *$(int* statusPtr) = (int)status;
                if (status != CURLM_OK) {
                    curl_multi_cleanup(multi);
                    return NULL;
                }
                return multi;
            } |]
        status <- fromIntegral <$> peek statusPtr
        if multi == nullPtr
            then throwIO $ CurlMultiInitFailed status
            else pure multi
  where
    checkedNatural name value
        | toInteger value > toInteger (maxBound :: CLong) =
            throwIO . userError $ "hcurl: " <> name <> " exceeds C long"
        | otherwise = pure $ fromIntegral value

cleanupCurlMulti :: FunPtr (Ptr CurlMulti -> IO ())
cleanupCurlMulti = [C.funPtr| void free_curl_multi(CURLM *ptr) { (void)curl_multi_cleanup(ptr); } |]
