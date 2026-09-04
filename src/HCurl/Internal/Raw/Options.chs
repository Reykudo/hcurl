module HCurl.Internal.Raw.Options where

#include "options.h"

-- Constructor spellings come from our underscore-separated C tags.
{# enum hcurl_easy_option as InternalOption {underscoreToCase}
    with prefix = "HCURL_OPT_" add prefix = "Internal_Option"
    deriving (Eq, Ord, Show) #}
