{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -lcurl #-}

module HCurl.Internal.Options (module HCurl.Internal.Options, module HCurl.Internal.Options.Class) where

import Control.DeepSeq
import Data.Singletons
import HCurl.Internal.Options.Class
import HCurl.Internal.Options.TH
import HCurl.Internal.Raw.Curl

data SomeOption where
    SomeOption' :: (SingI opt, EasyOption opt) => !(EasyOption' opt) -> SomeOption

instance NFData SomeOption where
    rnf = rwhnf

newtype EasyOption' (opt :: CurlOption) = EasyOption' (CurlParamBaseType opt)

instance (SingI opt, EasyOption opt) => EasyOption (EasyOption' opt) where
    type CurlParamBaseType (EasyOption' opt) = EasyOption' opt
    setEasyOption easyPtr (EasyOption' smth) = setEasyOption @opt easyPtr smth
    {-# INLINE setEasyOption #-}

genEasyOptionEnumInstances
    [ (''HTTPVersion, 'EasyHttpVersion)
    ]

genEasyOptionBoolInstances
    [ 'EasyHttpget
    , 'EasyNobody
    , 'EasyPost
    , 'EasyPipewait
    , 'EasyFollowlocation
    , 'EasyNosignal
    , 'EasyNoprogress
    , 'EasyIpresolve
    , 'EasySslVerifypeer
    , 'EasySslVerifyhost
    , 'EasyTcpFastopen
    , 'EasyTcpKeepalive
    ]

genEasyOptionLongInstances
    [ 'EasyTimeoutMs
    , 'EasyConnecttimeoutMs
    , 'EasyLowSpeedTime
    , 'EasyLowSpeedLimit
    , 'EasyPostfieldsize
    ]

genEasyOptionStringInstances
    [ 'EasyUrl
    , 'EasyAcceptEncoding
    , 'EasyPostfields
    , 'EasyCopypostfields
    ]
