{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Result where

import Control.Concurrent.MVar (MVar)
import Control.Exception (mask_, onException, throwIO)
import Foreign
import Foreign.C.Types (CInt)
import GHC.Conc (myThreadId, newStablePtrPrimMVar, threadCapability)
import HCurl.Internal.Raw.Context
import HCurl.Internal.Raw.Curl
import HCurl.Internal.Raw.Extras
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> localCtx)

C.include "extras.h"
C.include "HsFFI.h"

finalizerEasyData :: FunPtr (Ptr EasyData -> IO ())
finalizerEasyData =
    [C.funPtr| void hcurl_easy_data_finalizer(hs_easy_data_t *data) {
        hs_easy_data_destroy(data);
    } |]

{- | Create the one-shot result cell used by the C agent.  Once submitted,
the StablePtr belongs to @hs_try_putmvar@; if allocation fails before submit,
we still own and free it here.
-}
mkEasyData :: MVar () -> IO EasyData
mkEasyData waker = mask_ do
    (capability, _locked) <- threadCapability =<< myThreadId
    stable <- newStablePtrPrimMVar waker
    let stablePtr = castStablePtrToPtr stable
        capabilityC = fromIntegral capability :: CInt
    ptr <-
        [CU.exp| hs_easy_data_t* {
            hs_easy_data_create((HsStablePtr)$(void* stablePtr), $(int capabilityC))
        } |]
    if ptr == nullPtr
        then freeStablePtr stable >> throwIO (userError "hcurl: unable to allocate transfer result")
        else
            (EasyData <$> newForeignPtr finalizerEasyData ptr)
                `onException` [CU.block| void { hs_easy_data_destroy($(hs_easy_data_t* ptr)); } |]

getCurlCode :: EasyData -> IO CurlCode
getCurlCode easyData = withEasyData easyData \ptr -> do
    code <- [CU.exp| int { (int)hs_easy_data_code($(hs_easy_data_t* ptr)) } |]
    pure . toEnum $ fromIntegral code

getResponseCode :: EasyData -> IO Int
getResponseCode easyData = withEasyData easyData \ptr ->
    fromIntegral <$> [CU.exp| long { hs_easy_data_response_code($(hs_easy_data_t* ptr)) } |]
