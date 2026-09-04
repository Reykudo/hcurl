module HCurl.Internal.Raw.Headers where

#include "headers.h"

{# pointer *header_data_t as HeadersData foreign newtype #}

{# enum header_wait_result as HeaderWaitResult {underscoreToCase}
    with prefix = "HEADER_WAIT_" add prefix = "Header_Wait"
    deriving (Eq, Ord, Show) #}
