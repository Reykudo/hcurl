module HCurl.Internal.Waiter (
    WaitRegistration (..),
    awaitOneShot,
) where

import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar)
import Control.Exception (mask, onException)
import Control.Monad (when)
import Foreign (Ptr, castStablePtrToPtr, freeStablePtr)
import Foreign.C.Types (CInt)
import GHC.Conc (myThreadId, newStablePtrPrimMVar, threadCapability)

-- | Result of atomically offering a one-shot MVar waker to C.
data WaitRegistration
    = WaitRegistered
    | WaitReady
    | WaitRetry
    | WaitInvalid
    deriving (Eq, Show)

{- | Register and await a one-shot C-to-RTS MVar wake-up.

The registration action receives a 'newStablePtrPrimMVar' pointer and the
current capability.  On 'WaitRegistered', C owns that pointer until either it
fires it through @hs_try_putmvar@ or the unregister action explicitly returns
ownership.  Immediate outcomes leave ownership here and free the pointer.

This helper adds no FFI calls: callers supply the existing register and
unregister calls.  A fired registration is returned as 'WaitReady'.
-}
awaitOneShot :: (Ptr () -> CInt -> IO WaitRegistration) -> (Ptr () -> IO Bool) -> IO WaitRegistration
awaitOneShot register unregister = mask \restore -> do
    ready <- newEmptyMVar :: IO (MVar ())
    (capability, _locked) <- threadCapability =<< myThreadId
    stable <- newStablePtrPrimMVar ready
    let stablePtr = castStablePtrToPtr stable
        capabilityC = fromIntegral capability
    registration <- register stablePtr capabilityC `onException` freeStablePtr stable
    case registration of
        WaitRegistered -> do
            restore (takeMVar ready)
                `onException` do
                    returned <- unregister stablePtr
                    when returned $ freeStablePtr stable
            pure WaitReady
        immediate -> freeStablePtr stable >> pure immediate
