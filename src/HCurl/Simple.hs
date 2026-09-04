module HCurl.Simple (
    httpLBS,
    initCurl,
)
where

import Data.ByteString.Lazy qualified as BSL
import HCurl.Agent (Agent)
import HCurl.Internal.Easy (RequestBodyMode (UseRequestBody), RequestHandler (..), setSimpleStringResponse)
import HCurl.Internal.Multi (initCurlGlobal)
import HCurl.Internal.Raw.MPSC (noTransferStreams)
import HCurl.Internal.Response (getBufferedResponse)
import HCurl.Internal.Transfer (RunningTransfer (..), startTransferWith)
import HCurl.Request (Request)
import HCurl.Response (Response)
import HCurl.Types (CurlCode)
import UnliftIO (MonadUnliftIO, finally, liftIO, mask)
import UnliftIO.Resource (MonadResource)

initCurl :: IO ()
initCurl = initCurlGlobal

httpLBS :: (MonadResource m, MonadUnliftIO m) => Agent -> Request -> m (Either CurlCode (Response BSL.ByteString))
httpLBS agent request = mask \restore -> do
    transfer <-
        startTransferWith UseRequestBody agent request \_context _identifier easy _headers _metrics -> do
            target <- setSimpleStringResponse easy
            pure (target, [], noTransferStreams)
    let handler = transfer.transferHandler
    restore (liftIO $ getBufferedResponse handler handler.responseTarget)
        `finally` liftIO transfer.closeTransfer
