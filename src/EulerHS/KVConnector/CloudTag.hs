{-# LANGUAGE OverloadedStrings #-}

module EulerHS.KVConnector.CloudTag
  ( tagGuidWithCloud,
    cloudCodeOfGuid,
    preferredConnForGuid,
    cloudNameToCode,
    localCloudCode,
  )
where

import Data.Char (digitToInt, intToDigit, isDigit)
import Data.List (lookup)
import qualified Data.Text as T
import EulerHS.Prelude
import System.Environment (lookupEnv)
import System.IO.Unsafe (unsafePerformIO)

cloudNameToCode :: String -> Maybe Int
cloudNameToCode = \case
  "AWS" -> Just 0
  "GCP" -> Just 1
  "NAMMA" -> Just 2
  _ -> Nothing

localCloudCode :: Maybe Int
localCloudCode = unsafePerformIO $ (>>= cloudNameToCode) <$> lookupEnv "CLOUD_TYPE"
{-# NOINLINE localCloudCode #-}

isGuidShaped :: Text -> Bool
isGuidShaped t = T.length t == 36 && T.count "-" t == 4

tagGuidWithCloud :: Int -> Text -> Text
tagGuidWithCloud cloudCode guid
  | isGuidShaped guid = T.dropEnd 1 guid <> T.singleton (intToDigit cloudCode)
  | otherwise = guid

cloudCodeOfGuid :: Text -> Maybe Int
cloudCodeOfGuid guid
  | not (isGuidShaped guid) = Nothing
  | otherwise =
    let lastChar = T.last guid
     in if isDigit lastChar then Just (digitToInt lastChar) else Nothing

preferredConnForGuid :: [(Int, Text)] -> Text -> Maybe Text
preferredConnForGuid codes guid = cloudCodeOfGuid guid >>= \c -> lookup c codes
