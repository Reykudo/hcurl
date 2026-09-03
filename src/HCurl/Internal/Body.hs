{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Body (
    BodyReader,
    BodyStreamState,
    BodyStreamTarget,
    InvalidStreamBufferSize (..),
    StreamConfig (..),
    awaitBodyCompletion,
    awaitBodyStart,
    closeBody,
    defaultStreamConfig,
    finishBodyStream,
    installBodyStream,
    mkBodyReader,
    newBodyStreamState,
    publishBodyHead,
    readBody,
)
where

import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Trans.Resource (MonadResource, ReleaseKey, register)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Function ((&))
import Data.Functor (($>))
import Data.Maybe (isNothing)
import Foreign.C.Types
import Foreign.Ptr
import HCurl.Internal.Metrics (Metrics)
import HCurl.Internal.Raw
import HCurl.Response (HttpParts)
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU
import Numeric.Natural (Natural)

C.context (C.baseCtx <> C.funCtx <> C.fptrCtx <> C.bsCtx <> localCtx)

C.include "<curl/curl.h>"

data StreamConfig = StreamConfig
    { bufferedChunks :: !Natural
    }
    deriving (Show, Eq)

defaultStreamConfig :: StreamConfig
defaultStreamConfig = StreamConfig{bufferedChunks = 16}

data InvalidStreamBufferSize = InvalidStreamBufferSize
    deriving (Show)

instance Exception InvalidStreamBufferSize

data BodyTerminal
    = BodyCurl !CurlCode
    | BodyException !SomeException

data BodyStreamState = BodyStreamState
    { chunks :: !(TBQueue ByteString)
    , headResult :: !(TMVar HttpParts)
    , completionResult :: !(TMVar (Either SomeException (Either CurlCode Metrics)))
    , terminalResult :: !(TVar (Maybe BodyTerminal))
    , callbackFailure :: !(TVar (Maybe SomeException))
    , paused :: !(TVar Bool)
    , closed :: !(TVar Bool)
    }

data BodyReader = BodyReader
    { streamState :: !BodyStreamState
    , resumeTransfer :: !(IO ())
    , releaseTransfer :: !(IO ())
    , readLock :: !(MVar ())
    }

type WriteCallback = Ptr CChar -> CSize -> CSize -> Ptr () -> IO CSize

newtype BodyStreamTarget = BodyStreamTarget (FunPtr WriteCallback)

foreign import ccall "wrapper"
    makeWriteCallback :: WriteCallback -> IO (FunPtr WriteCallback)

foreign import capi "curl/curl.h value CURL_WRITEFUNC_PAUSE"
    curlWriteFuncPause :: CSize

newBodyStreamState :: StreamConfig -> IO BodyStreamState
newBodyStreamState StreamConfig{bufferedChunks}
    | bufferedChunks == 0 = throwIO InvalidStreamBufferSize
    | otherwise = do
        chunks <- newTBQueueIO bufferedChunks
        headResult <- newEmptyTMVarIO
        completionResult <- newEmptyTMVarIO
        terminalResult <- newTVarIO Nothing
        callbackFailure <- newTVarIO Nothing
        paused <- newTVarIO False
        closed <- newTVarIO False
        pure BodyStreamState{..}

publishBodyHead :: BodyStreamState -> HttpParts -> IO ()
publishBodyHead BodyStreamState{headResult} responseHead =
    atomically . void $ tryPutTMVar headResult responseHead

recordCallbackFailure :: BodyStreamState -> SomeException -> IO ()
recordCallbackFailure BodyStreamState{callbackFailure} exception =
    atomically do
        current <- readTVar callbackFailure
        when (isNothing current) $ writeTVar callbackFailure (Just exception)

data WriteDecision = AcceptChunk | PauseTransfer | AbortTransfer

chooseWriteAction :: BodyStreamState -> STM WriteDecision
chooseWriteAction BodyStreamState{chunks, paused, closed} = do
    isClosed <- readTVar closed
    if isClosed
        then pure AbortTransfer
        else do
            isFull <- isFullTBQueue chunks
            if isFull
                then writeTVar paused True $> PauseTransfer
                else pure AcceptChunk

bodyWriteCallback :: BodyStreamState -> IO HttpParts -> WriteCallback
bodyWriteCallback state@BodyStreamState{chunks, headResult, closed} loadHead ptr size count _userdata = mask_ do
    let totalSize = size * count
    if totalSize == 0
        then pure 0
        else handle onFailure do
            chooseWriteAction state & atomically >>= \case
                AbortTransfer -> pure 0
                PauseTransfer -> pure curlWriteFuncPause
                AcceptChunk -> do
                    needsHead <- atomically $ isEmptyTMVar headResult
                    when needsHead $ loadHead >>= publishBodyHead state
                    if toInteger totalSize > toInteger (maxBound :: Int)
                        then throwIO $ userError "libcurl body chunk exceeds Haskell Int"
                        else do
                            chunk <- BS.packCStringLen (ptr, fromIntegral totalSize)
                            accepted <- atomically do
                                isClosed <- readTVar closed
                                unless isClosed $ writeTBQueue chunks chunk
                                pure $ not isClosed
                            pure $ if accepted then totalSize else 0
  where
    onFailure exception = do
        recordCallbackFailure state exception
        pure 0

installBodyStream :: (MonadResource m) => CurlEasy -> BodyStreamState -> IO HttpParts -> m (BodyStreamTarget, [ReleaseKey])
installBodyStream (CurlEasy easyPtr) state loadHead = do
    callback <- liftIO $ makeWriteCallback (bodyWriteCallback state loadHead)
    let callbackPtr = castFunPtrToPtr callback
    liftIO
        [CU.block|void {
            CURL *easy = $(CURL* easyPtr);
            curl_easy_setopt(easy, CURLOPT_WRITEDATA, NULL);
            curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, $(void* callbackPtr));
        }|]
    releaseCallback <- register $ freeHaskellFunPtr callback
    pure (BodyStreamTarget callback, [releaseCallback])

finishBodyStream :: BodyStreamState -> Either SomeException (Either CurlCode (HttpParts, Metrics)) -> IO ()
finishBodyStream BodyStreamState{headResult, completionResult, terminalResult, callbackFailure, paused} result =
    atomically do
        alreadyFinished <- not <$> isEmptyTMVar completionResult
        unless alreadyFinished do
            recordedFailure <- readTVar callbackFailure
            let finalResult = maybe result Left recordedFailure
            case finalResult of
                Left exception -> do
                    writeTVar terminalResult . Just $ BodyException exception
                    putTMVar completionResult $ Left exception
                Right (Left curlCode) -> do
                    writeTVar terminalResult . Just $ BodyCurl curlCode
                    putTMVar completionResult . Right $ Left curlCode
                Right (Right (responseHead, metrics)) -> do
                    void $ tryPutTMVar headResult responseHead
                    writeTVar terminalResult . Just $ BodyCurl Ok
                    putTMVar completionResult . Right $ Right metrics
            writeTVar paused False

awaitBodyStart :: BodyStreamState -> IO (Either CurlCode HttpParts)
awaitBodyStart BodyStreamState{headResult, completionResult} = do
    result <-
        atomically $
            (Right . Right <$> readTMVar headResult)
                `orElse` do
                    completion <- readTMVar completionResult
                    pure case completion of
                        Left exception -> Left exception
                        Right (Left curlCode) -> Right $ Left curlCode
                        Right (Right _) -> error "hcurl: successful completion without response headers"
    either throwIO pure result

awaitBodyCompletion :: BodyStreamState -> IO (Either CurlCode Metrics)
awaitBodyCompletion BodyStreamState{completionResult} = do
    result <- atomically $ readTMVar completionResult
    either throwIO pure result

mkBodyReader :: BodyStreamState -> IO () -> IO () -> IO BodyReader
mkBodyReader streamState resumeTransfer releaseTransfer = do
    readLock <- newMVar ()
    pure BodyReader{..}

data ReadResult
    = ReadChunk !ByteString !Bool
    | ReadEnd !BodyTerminal
    | ReadClosed

nextReadResult :: BodyStreamState -> STM ReadResult
nextReadResult BodyStreamState{chunks, terminalResult, paused, closed} = do
    nextChunk <- tryReadTBQueue chunks
    case nextChunk of
        Just chunk -> do
            terminal <- readTVar terminalResult
            isPaused <- readTVar paused
            isClosed <- readTVar closed
            let shouldResume = isPaused && isNothing terminal && not isClosed
            when shouldResume $ writeTVar paused False
            pure $ ReadChunk chunk shouldResume
        Nothing -> do
            isClosed <- readTVar closed
            if isClosed
                then pure ReadClosed
                else readTVar terminalResult >>= maybe retry (pure . ReadEnd)

{- | Read one response-body chunk.  'Right Nothing' means a successful end of
stream.  A libcurl failure is returned explicitly; internal exceptions are
rethrown.
-}
readBody :: BodyReader -> IO (Either CurlCode (Maybe ByteString))
readBody BodyReader{streamState, resumeTransfer, releaseTransfer, readLock} =
    withMVar readLock \_ -> do
        atomically (nextReadResult streamState) >>= \case
            ReadChunk chunk shouldResume -> do
                when shouldResume $
                    resumeTransfer `onException` atomically (writeTVar streamState.paused True)
                pure . Right $ Just chunk
            ReadEnd (BodyCurl Ok) -> releaseTransfer $> Right Nothing
            ReadEnd (BodyCurl curlCode) -> releaseTransfer $> Left curlCode
            ReadEnd (BodyException exception) -> releaseTransfer >> throwIO exception
            ReadClosed -> pure $ Left AbortedByCallback

{- | Stop a response early and release its libcurl resources.  It is safe to
call this more than once.
-}
closeBody :: BodyReader -> IO ()
closeBody BodyReader{streamState = BodyStreamState{chunks, closed}, releaseTransfer} = mask_ do
    atomically do
        writeTVar closed True
        void $ flushTBQueue chunks
    releaseTransfer
