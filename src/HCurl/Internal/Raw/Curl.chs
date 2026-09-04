{-# OPTIONS_GHC -Wno-name-shadowing #-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE CApiFFI #-}

module HCurl.Internal.Raw.Curl where

import Control.Exception (Exception)
import Control.DeepSeq
import Data.List (elemIndex)
import GHC.Generics
import Data.Singletons.TH (genSingletons)

#include <curl/curl.h>

#include "simple_string.h"

{# context lib="curl" #}

{# pointer *CURLM as CurlMulti foreign newtype #}

{# pointer *CURL as CurlEasy newtype #}

{# pointer *curl_slist as CurlSlist foreign newtype #}

instance NFData CurlSlist where
    rnf = rwhnf

{# enum CURLoption as CurlOption {underscoreToCase} omit (CURLOPT_OBSOLETE40, CURLOPT_LASTENTRY) with prefix = "CURLOPT_" add prefix = "Easy" deriving (Eq, Ord, Show, Generic) #}

$(genSingletons [''CurlOption])

deriving anyclass instance NFData CurlOption

-- Keep transfer failures independent from the installed headers. libcurl has
-- kept CURLcode's numeric ABI stable, but it appends and occasionally renames
-- enum members. A c2hs-generated sum type therefore changed hcurl's API with
-- the build headers and made a newer runtime code crash 'toEnum'.
data CurlCode
    = Ok
    | UnsupportedProtocol
    | FailedInit
    | UrlMalformat
    | NotBuiltIn
    | CouldntResolveProxy
    | CouldntResolveHost
    | CouldntConnect
    | WeirdServerReply
    | RemoteAccessDenied
    | FtpAcceptFailed
    | FtpWeirdPassReply
    | FtpAcceptTimeout
    | FtpWeirdPasvReply
    | FtpWeird227Format
    | FtpCantGetHost
    | Http2
    | FtpCouldntSetType
    | PartialFile
    | FtpCouldntRetrFile
    | Obsolete20
    | QuoteError
    | HttpReturnedError
    | WriteError
    | Obsolete24
    | UploadFailed
    | ReadError
    | OutOfMemory
    | OperationTimedout
    | Obsolete29
    | FtpPortFailed
    | FtpCouldntUseRest
    | Obsolete32
    | RangeError
    | Obsolete34
    | SslConnectError
    | BadDownloadResume
    | FileCouldntReadFile
    | LdapCannotBind
    | LdapSearchFailed
    | Obsolete40
    | Obsolete41
    | AbortedByCallback
    | BadFunctionArgument
    | Obsolete44
    | InterfaceFailed
    | Obsolete46
    | TooManyRedirects
    | UnknownOption
    | SetoptOptionSyntax
    | Obsolete50
    | Obsolete51
    | GotNothing
    | SslEngineNotfound
    | SslEngineSetfailed
    | SendError
    | RecvError
    | Obsolete57
    | SslCertproblem
    | SslCipher
    | PeerFailedVerification
    | BadContentEncoding
    | Obsolete62
    | FilesizeExceeded
    | UseSslFailed
    | SendFailRewind
    | SslEngineInitfailed
    | LoginDenied
    | TftpNotfound
    | TftpPerm
    | RemoteDiskFull
    | TftpIllegal
    | TftpUnknownid
    | RemoteFileExists
    | TftpNosuchuser
    | Obsolete75
    | Obsolete76
    | SslCacertBadfile
    | RemoteFileNotFound
    | Ssh
    | SslShutdownFailed
    | Again
    | SslCrlBadfile
    | SslIssuerError
    | FtpPretFailed
    | RtspCseqError
    | RtspSessionError
    | FtpBadFileList
    | ChunkFailed
    | NoConnectionAvailable
    | SslPinnedpubkeynotmatch
    | SslInvalidcertstatus
    | Http2Stream
    | RecursiveApiCall
    | AuthError
    | Http3
    | QuicConnectError
    | Proxy
    | SslClientcert
    | UnrecoverablePoll
    | TooLarge
    | EchRequired
    | UnknownCurlCode !Int
    deriving (Eq, Ord, Show, Generic)

knownCurlCodes :: [CurlCode]
knownCurlCodes =
    [ Ok
    , UnsupportedProtocol
    , FailedInit
    , UrlMalformat
    , NotBuiltIn
    , CouldntResolveProxy
    , CouldntResolveHost
    , CouldntConnect
    , WeirdServerReply
    , RemoteAccessDenied
    , FtpAcceptFailed
    , FtpWeirdPassReply
    , FtpAcceptTimeout
    , FtpWeirdPasvReply
    , FtpWeird227Format
    , FtpCantGetHost
    , Http2
    , FtpCouldntSetType
    , PartialFile
    , FtpCouldntRetrFile
    , Obsolete20
    , QuoteError
    , HttpReturnedError
    , WriteError
    , Obsolete24
    , UploadFailed
    , ReadError
    , OutOfMemory
    , OperationTimedout
    , Obsolete29
    , FtpPortFailed
    , FtpCouldntUseRest
    , Obsolete32
    , RangeError
    , Obsolete34
    , SslConnectError
    , BadDownloadResume
    , FileCouldntReadFile
    , LdapCannotBind
    , LdapSearchFailed
    , Obsolete40
    , Obsolete41
    , AbortedByCallback
    , BadFunctionArgument
    , Obsolete44
    , InterfaceFailed
    , Obsolete46
    , TooManyRedirects
    , UnknownOption
    , SetoptOptionSyntax
    , Obsolete50
    , Obsolete51
    , GotNothing
    , SslEngineNotfound
    , SslEngineSetfailed
    , SendError
    , RecvError
    , Obsolete57
    , SslCertproblem
    , SslCipher
    , PeerFailedVerification
    , BadContentEncoding
    , Obsolete62
    , FilesizeExceeded
    , UseSslFailed
    , SendFailRewind
    , SslEngineInitfailed
    , LoginDenied
    , TftpNotfound
    , TftpPerm
    , RemoteDiskFull
    , TftpIllegal
    , TftpUnknownid
    , RemoteFileExists
    , TftpNosuchuser
    , Obsolete75
    , Obsolete76
    , SslCacertBadfile
    , RemoteFileNotFound
    , Ssh
    , SslShutdownFailed
    , Again
    , SslCrlBadfile
    , SslIssuerError
    , FtpPretFailed
    , RtspCseqError
    , RtspSessionError
    , FtpBadFileList
    , ChunkFailed
    , NoConnectionAvailable
    , SslPinnedpubkeynotmatch
    , SslInvalidcertstatus
    , Http2Stream
    , RecursiveApiCall
    , AuthError
    , Http3
    , QuicConnectError
    , Proxy
    , SslClientcert
    , UnrecoverablePoll
    , TooLarge
    , EchRequired
    ]

instance Enum CurlCode where
    toEnum value
        | value < 0 = UnknownCurlCode value
        | otherwise =
            case drop value knownCurlCodes of
                code : _ -> code
                [] -> UnknownCurlCode value

    fromEnum (UnknownCurlCode value) = value
    fromEnum code =
        case elemIndex code knownCurlCodes of
            Just value -> value
            Nothing -> error "CurlCode.fromEnum: missing known code"

deriving anyclass instance Exception CurlCode
deriving anyclass instance NFData CurlCode

-- Kept independent from the installed headers so the module still compiles
-- with pre-HTTP/2 and pre-HTTP/3 libcurl.  Unsupported values are rejected by
-- curl_easy_setopt at runtime.  These numeric values are ABI-stable in curl.
data HTTPVersion
    = HTTP_Default
    | HTTP_1_0
    | HTTP_1_1
    | HTTP_2
    | HTTP_2_TLS
    | HTTP_2_NoUpgrade
    | HTTP_3
    | HTTP_3_Only
    deriving (Eq, Ord, Show, Generic)

httpVersionValue :: HTTPVersion -> Int
httpVersionValue = \case
    HTTP_Default -> 0
    HTTP_1_0 -> 1
    HTTP_1_1 -> 2
    HTTP_2 -> 3
    HTTP_2_TLS -> 4
    HTTP_2_NoUpgrade -> 5
    HTTP_3 -> 30
    HTTP_3_Only -> 31

deriving anyclass instance NFData HTTPVersion
