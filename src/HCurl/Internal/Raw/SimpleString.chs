module HCurl.Internal.Raw.SimpleString where

#include "simple_string.h"

{# pointer *simple_string_t as SimpleStringPtr foreign newtype #}
