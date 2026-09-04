module HCurl.Internal.Once (
    Once,
    newOnce,
    newOnceState,
    runOnce,
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (void)

{- | Shared state for an action whose result (including failure) is memoized.
The first caller performs the action; concurrent callers wait on the same
MVar. The action must be idempotent: if its owner is interrupted or fails,
cleanup is retried once on a replacement thread while the original caller
still receives the first exception. Normal successful calls create no
helper thread.
-}
data Once
    = OncePending
    | OnceRunning !(MVar (Either SomeException ()))
    | OnceDone !(Either SomeException ())

data OnceClaim
    = OwnRun !(MVar (Either SomeException ()))
    | WaitForRun !(MVar (Either SomeException ()))
    | UseResult !(Either SomeException ())

newOnceState :: IO (MVar Once)
newOnceState = newMVar OncePending

newOnce :: IO () -> IO (IO ())
newOnce action = do
    state <- newOnceState
    pure $ runOnce state action

runOnce :: MVar Once -> IO () -> IO ()
runOnce state action = mask \restore -> do
    claim <- modifyMVar state \case
        OncePending -> do
            resultCell <- newEmptyMVar
            pure (OnceRunning resultCell, OwnRun resultCell)
        running@(OnceRunning resultCell) -> pure (running, WaitForRun resultCell)
        done@(OnceDone result) -> pure (done, UseResult result)
    case claim of
        OwnRun resultCell -> do
            outcome <- try @SomeException $ restore action
            case outcome of
                Left exception -> do
                    forkOutcome <- try @SomeException . forkIO $ do
                        retried <- try @SomeException $ restore action
                        finishOnce state resultCell retried
                    case forkOutcome of
                        Left forkException ->
                            finishOnce state resultCell (Left forkException)
                        Right _ -> pure ()
                    throwIO exception
                Right () -> finishOnce state resultCell outcome
        WaitForRun resultCell -> restore (readMVar resultCell) >>= rethrow
        UseResult result -> rethrow result

finishOnce :: MVar Once -> MVar (Either SomeException ()) -> Either SomeException () -> IO ()
finishOnce state resultCell result = mask_ do
    modifyMVar_ state $ const $ pure (OnceDone result)
    void $ tryPutMVar resultCell result

rethrow :: Either SomeException () -> IO ()
rethrow = either throwIO pure
