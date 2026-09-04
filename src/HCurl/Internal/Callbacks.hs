{- | Reserved compatibility module. Transfer callbacks are deliberately
implemented in C so libcurl never enters Haskell synchronously.
-}
module HCurl.Internal.Callbacks where
