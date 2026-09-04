module HCurl.Internal.Raw.Stream where

import Foreign.C.Types (CInt)

#include "stream.h"

{# pointer *hcurl_stream_t as CurlStream foreign newtype #}

{# enum hcurl_stream_kind as StreamKind {underscoreToCase}
    with prefix = "HCURL_STREAM_" add prefix = "Stream"
    deriving (Eq, Ord, Show) #}

{# enum hcurl_stream_read_result as StreamReadResult {underscoreToCase}
    with prefix = "HCURL_STREAM_READ_" add prefix = "Stream_Read"
    deriving (Eq, Ord, Show) #}

{# enum hcurl_stream_wait_result as StreamWaitResult {underscoreToCase}
    with prefix = "HCURL_STREAM_WAIT_" add prefix = "Stream_Wait"
    deriving (Eq, Ord, Show) #}

{# enum hcurl_stream_write_result as StreamWriteResult {underscoreToCase}
    with prefix = "HCURL_STREAM_WRITE_" add prefix = "Stream_Write"
    deriving (Eq, Ord, Show) #}

{# enum hcurl_stream_control_result as StreamControlResult {underscoreToCase}
    with prefix = "HCURL_STREAM_CONTROL_" add prefix = "Stream_Control"
    deriving (Eq, Ord, Show) #}

streamUploadUnsupported :: CInt
streamUploadUnsupported = {# const HCURL_STREAM_UPLOAD_UNSUPPORTED #}
