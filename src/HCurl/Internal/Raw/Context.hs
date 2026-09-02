{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskellQuotes #-}

module HCurl.Internal.Raw.Context where

import Data.Map qualified as Map
import Language.Haskell.TH qualified as TH

import HCurl.Internal.Raw.Curl
import HCurl.Internal.Raw.Extras
import HCurl.Internal.Raw.Headers
import HCurl.Internal.Raw.MPSC
import HCurl.Internal.Raw.Metrics
import HCurl.Internal.Raw.SimpleString
import HCurl.Internal.Raw.UV
import Language.C.Inline.Context
import Language.C.Types qualified as C

localCtx :: Context
localCtx =
    mempty
        { ctxTypesTable = curlTypesTable <> extraTypesTable <> libuvTypesTable
        }

curlTypesTable :: Map.Map C.TypeSpecifier TH.TypeQ
curlTypesTable =
    Map.fromList
        [ (C.TypeName "CURLM", [t|CurlMulti|])
        , (C.TypeName "CURL", [t|CurlEasy|])
        , (C.TypeName "curl_slist_t", [t|CurlSlist|])
        ]

extraTypesTable :: Map.Map C.TypeSpecifier TH.TypeQ
extraTypesTable =
    Map.fromList
        [ (C.TypeName "hs_easy_data_t", [t|EasyData|])
        , (C.TypeName "simple_string_t", [t|SimpleString|])
        , (C.TypeName "mpsc_t", [t|MPSCQ|])
        , (C.TypeName "outer_message_t", [t|InternalOuterMessage|])
        , (C.TypeName "curl_metrics_context_t", [t|CurlMetricsContext|])
        , (C.TypeName "header_data_t", [t|HeadersData|])
        ]

libuvTypesTable :: Map.Map C.TypeSpecifier TH.TypeQ
libuvTypesTable =
    Map.fromList
        [ (C.TypeName "uv_async_t", [t|UVAsync|])
        , (C.TypeName "uv_loop_t", [t|UVLoop|])
        ]
