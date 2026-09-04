{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module HCurl.Internal.Raw.MPSC where

import Foreign (Ptr)
import Data.Word (Word64)
import HCurl.Internal.Raw.Curl
import HCurl.Internal.Raw.Extras
import HCurl.Internal.Raw.Metrics
import HCurl.Internal.Raw.Stream

#include "message_chan.h"

{# pointer *mpsc_t as MPSCQ foreign newtype #}

{# pointer *message_sender_t as MessageSender foreign newtype #}

{# enum outer_message_types as InternalOuterMessageTag {underscoreToCase} add prefix = "Internal" deriving (Eq) #}

newtype TransferId = TransferId {unTransferId :: Word64}
    deriving (Show, Eq, Ord)

data TransferStreams = TransferStreams
    { downloadStream :: !(Maybe CurlStream)
    , uploadStream :: !(Maybe CurlStream)
    }

noTransferStreams :: TransferStreams
noTransferStreams = TransferStreams Nothing Nothing

data OuterMessage
    = Execute !TransferId !(Ptr CurlEasy) !EasyData !CurlMetricsContext !TransferStreams
    | CancelRequest !TransferId
    | ResumeRequest !TransferId
    | StopAgent
