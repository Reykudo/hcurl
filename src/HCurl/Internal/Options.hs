{-# LANGUAGE CApiFFI #-}

module HCurl.Internal.Options (
    module HCurl.Internal.Options.Class,
    module HCurl.Options,
    setSomeOption,
) where

import Control.Exception (throwIO)
import Control.Monad (unless, when)
import Data.ByteString qualified as BS
import Foreign.C.Types (CInt, CLong)
import Foreign.Ptr (Ptr)
import HCurl.Internal.Options.Class
import HCurl.Internal.Raw.Curl
import HCurl.Internal.Raw.CurlFunctions (
    hcurl_easy_setopt_long,
    hcurl_easy_setopt_string,
 )
import HCurl.Internal.Raw.Options
import HCurl.Options

setSomeOption :: Ptr CurlEasy -> SomeOption -> IO ()
setSomeOption easyPtr = \case
    OptionHttpVersion version ->
        setLong InternalOptionHttpVersion $ fromIntegral (httpVersionValue version)
    OptionPipeWait value -> setBool InternalOptionPipeWait value
    OptionFollowLocation value -> setBool InternalOptionFollowLocation value
    OptionNoSignal value -> setBool InternalOptionNoSignal value
    OptionSslVerifyPeer value -> setBool InternalOptionSslVerifyPeer value
    OptionTcpFastOpen value -> setBool InternalOptionTcpFastOpen value
    OptionTcpKeepAlive value -> setBool InternalOptionTcpKeepAlive value
    OptionTimeoutMs value -> setInt "timeout" InternalOptionTimeoutMs value
    OptionConnectTimeoutMs value -> setInt "connect timeout" InternalOptionConnectTimeoutMs value
    OptionLowSpeedTime value -> setInt "low speed time" InternalOptionLowSpeedTime value
    OptionLowSpeedLimit value -> setInt "low speed limit" InternalOptionLowSpeedLimit value
    OptionAcceptEncoding value -> setString "accept encoding" InternalOptionAcceptEncoding value
    OptionIpResolve value -> setLong InternalOptionIpResolve $ fromIntegral (fromEnum value)
    OptionSslVerifyHost value ->
        setLong InternalOptionSslVerifyHost $ if value then 2 else 0
  where
    tagValue = fromIntegral . fromEnum

    setBool tag value = setLong tag $ if value then 1 else 0

    setInt name tag value = do
        let integerValue = toInteger value
        when (integerValue < 0) $
            throwIO $
                OptionIntegerIsNegative name integerValue
        when
            (integerValue > toInteger (maxBound :: CLong))
            $ throwIO
            $ OptionIntegerOutOfRange name integerValue
        setLong tag $ fromIntegral value

    setLong tag value = do
        code <- hcurl_easy_setopt_long easyPtr (tagValue tag) value
        checkCode code

    setString name tag value = do
        when (BS.elem 0 value) $ throwIO $ OptionStringContainsNul name
        when (BS.elem 10 value || BS.elem 13 value) $
            throwIO $
                OptionStringContainsNewline name
        BS.useAsCString value \valuePtr -> do
            code <- hcurl_easy_setopt_string easyPtr (tagValue tag) valuePtr
            checkCode code

    checkCode :: CInt -> IO ()
    checkCode code =
        unless (code == 0) $ throwIO (toEnum (fromIntegral code) :: CurlCode)
