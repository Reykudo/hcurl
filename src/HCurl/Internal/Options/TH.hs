{-# LANGUAGE TemplateHaskell #-}

module HCurl.Internal.Options.TH where

import Control.Exception (throwIO)
import Control.Monad
import Data.Singletons
import Foreign.C
import HCurl.Internal.Options.Class
import HCurl.Internal.Raw.Curl (CurlCode)
import HCurl.Internal.Raw.CurlFunctions
import Language.Haskell.TH

checkedOptionLong :: Int -> IO CLong
checkedOptionLong value
    | integerValue < toInteger (minBound :: CLong)
        || integerValue > toInteger (maxBound :: CLong) =
        throwIO $ userError "curl option integer exceeds C long"
    | otherwise = pure $ fromIntegral value
  where
    integerValue = toInteger value

genEasyOptionEnumInstances :: [(Name, Name)] -> Q [Dec]
genEasyOptionEnumInstances names = join <$> traverse genEasyOptionEnumInstance names

genEasyOptionEnumInstance :: (Name, Name) -> Q [Dec]
genEasyOptionEnumInstance (enumName, instanceType) =
    [d|
        instance EasyOption $instanceType' where
            type CurlParamBaseType $instanceType' = $enumName'
            setEasyOption easyPtr opt = do
                let longVal = fromIntegral . fromEnum $ opt
                let optVal = fromIntegral . fromEnum $ demote @($instanceType')
                code <- curl_easy_setopt_long easyPtr optVal longVal
                unless (code == 0) $ throwIO (toEnum (fromIntegral code) :: CurlCode)
            {-# INLINE setEasyOption #-}
        |]
  where
    instanceType' = pure (ConT instanceType)
    enumName' = pure (ConT enumName)

genEasyOptionBoolInstances :: [Name] -> Q [Dec]
genEasyOptionBoolInstances names = join <$> traverse (genEasyOptionEnumInstance . (''Bool,)) names

genEasyOptionLongInstances :: [Name] -> Q [Dec]
genEasyOptionLongInstances names = join <$> traverse genEasyOptionLongInstance names

genEasyOptionLongInstance :: Name -> Q [Dec]
genEasyOptionLongInstance instanceType =
    [d|
        instance EasyOption $instanceType' where
            type CurlParamBaseType $instanceType' = Int
            setEasyOption easyPtr opt = do
                longVal <- checkedOptionLong opt
                let optVal = fromIntegral . fromEnum $ demote @($instanceType')
                code <- curl_easy_setopt_long easyPtr optVal longVal
                unless (code == 0) $ throwIO (toEnum (fromIntegral code) :: CurlCode)
            {-# INLINE setEasyOption #-}
        |]
  where
    instanceType' = pure (ConT instanceType)
