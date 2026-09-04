{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Slist where

import Control.Exception
import Control.Monad (when)
import Control.Monad.Cont (ContT (..))
import Control.Monad.Trans
import Control.Monad.Trans.Resource (MonadResource, ReleaseKey, allocate)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Coerce
import Data.Foldable (for_)
import Foreign
import HCurl.Internal.Raw
import Language.C.Inline qualified as C
import Language.C.Inline.Unsafe qualified as CU

C.context (C.baseCtx <> localCtx)

C.include "<curl/curl.h>"
C.include "extras.h"

data CurlSlistError
    = CurlSlistAppendFailed
    | CurlSlistContainsNul !Int
    | CurlSlistContainsNewline !Int
    deriving (Show)
    deriving anyclass (Exception)

toHeaderSlistCont :: [ByteString] -> ContT a IO (Ptr CurlSlist)
toHeaderSlistCont headers = do
    headersC <- traverse (ContT . BS.useAsCString) headers
    (headersCArrLen', headersCArr) <- ContT (withArrayLen headersC . curry)
    let headersCArrLen = fromIntegral headersCArrLen'
    ptr <-
        lift
            [CU.block| curl_slist_t* {
                curl_slist_t* slist = NULL;

                for (size_t i = 0; i < $(size_t headersCArrLen); i++) {
                  curl_slist_t* temp = curl_slist_append(slist, $(char** headersCArr)[i]);
                  if (temp == NULL) {
                    curl_slist_free_all(slist);
                    return NULL;
                  }
                  slist = temp;
                }

                return slist;
        }|]

    if ptr == nullPtr
        then liftIO $ throwIO CurlSlistAppendFailed
        else pure ptr

toHeaderSlistP :: [ByteString] -> IO (Ptr CurlSlist)
toHeaderSlistP [] = pure nullPtr
toHeaderSlistP headers = do
    for_ (zip [0 ..] headers) \(index, header) ->
        if BS.elem 0 header
            then throwIO $ CurlSlistContainsNul index
            else
                when (BS.elem 10 header || BS.elem 13 header) $
                    throwIO $
                        CurlSlistContainsNewline index
    runContT (toHeaderSlistCont headers) pure

finalizeCurlSlist :: FunPtr (Ptr CurlSlist -> IO ())
finalizeCurlSlist = [C.funPtr| void free_slist(curl_slist_t* ptr){ curl_slist_free_all(ptr); } |]

-- | Manage memory with ResourceT
allocateSlist :: (MonadResource m) => [ByteString] -> m (ReleaseKey, Ptr CurlSlist)
allocateSlist headers =
    allocate
        (toHeaderSlistP headers)
        \ptr -> [CU.block|void {curl_slist_free_all($(curl_slist_t* ptr));}|]

{- | Add one request-local header in front of a borrowed reusable list unless
that list already contains the same (ASCII case-insensitive) header name.
Only the prefix is owned; the borrowed tail is kept alive by the release
action and is never copied or mutated.
-}
allocateSlistOverlayHeader :: (MonadResource m) => ForeignPtr CurlSlist -> ByteString -> m (ReleaseKey, Ptr CurlSlist)
allocateSlistOverlayHeader borrowed header = do
    when (BS.elem 0 header) $ liftIO $ throwIO $ CurlSlistContainsNul 0
    when (BS.elem 10 header || BS.elem 13 header) $
        liftIO $
            throwIO $
                CurlSlistContainsNewline 0
    allocate create destroy
  where
    create =
        withForeignPtr borrowed \borrowedPtr ->
            BS.useAsCString header \headerPtr -> do
                headPtr <-
                    [CU.exp| curl_slist_t* {
                        hcurl_slist_overlay_header(
                            $(curl_slist_t* borrowedPtr), $(char* headerPtr))
                    } |]
                when (headPtr == nullPtr) $ throwIO CurlSlistAppendFailed
                pure headPtr
    destroy headPtr =
        withForeignPtr borrowed \borrowedPtr ->
            [CU.block| void {
                hcurl_slist_free_overlay(
                    $(curl_slist_t* headPtr), $(curl_slist_t* borrowedPtr));
            } |]

-- | Manage memory with ForeignPtr
toHeaderSlist :: [ByteString] -> IO CurlSlist
toHeaderSlist headers = mask_ do
    ptr <- toHeaderSlistP headers
    coerce (newForeignPtr finalizeCurlSlist ptr)
        `onException` [CU.block| void { curl_slist_free_all($(curl_slist_t* ptr)); } |]
