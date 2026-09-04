{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.MPSC where

import Control.Exception (Exception, mask_, onException, throwIO)
import Foreign
import Foreign.C.Types
import HCurl.Internal.Raw
import HCurl.Internal.Raw.MPSC
import Language.C.Inline qualified as C

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "message_chan.h"
C.include "include/waitfree-mpsc-queue/mpscq.h"

newtype InvalidMPSCQueueCapacity = InvalidMPSCQueueCapacity Int
    deriving (Show)

instance Exception InvalidMPSCQueueCapacity

initMPSCQ :: Int -> IO MPSCQ
initMPSCQ capacity
    | capacity < 2
        || capacity .&. (capacity - 1) /= 0
        || toInteger capacity > toInteger (maxBound :: CInt) =
        throwIO $ InvalidMPSCQueueCapacity capacity
    | otherwise = mask_ do
        let capacity' = fromIntegral capacity
        ptr <- [C.exp|mpsc_t* { mpscq_create(NULL, $(int capacity'))}|]
        if ptr == nullPtr
            then throwIO $ userError "hcurl: unable to allocate agent message queue"
            else
                (MPSCQ <$> newForeignPtr finalizerMPSCQ ptr)
                    `onException` [C.block| void { mpscq_destroy($(mpsc_t* ptr)); } |]

finalizerMPSCQ :: FunPtr (Ptr MPSCQ -> IO ())
finalizerMPSCQ = [C.funPtr| void mpscq_finalizer(mpsc_t* ptr) { mpscq_destroy(ptr); } |]
