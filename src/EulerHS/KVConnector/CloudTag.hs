{-# LANGUAGE OverloadedStrings #-}

module EulerHS.KVConnector.CloudTag
  ( cloudTagModulus,
    tagGuidWithCloud,
    cloudCodeOfGuid,
    preferredConnForGuid,
  )
where

import Data.Char (digitToInt, intToDigit, isDigit)
import Data.List (lookup)
import qualified Data.Text as T
import EulerHS.Prelude

cloudTagModulus :: Int
cloudTagModulus = 10

isGuidShaped :: Text -> Bool
isGuidShaped t = T.length t == 36 && T.count "-" t == 4

tagGuidWithCloud :: Int -> Text -> Text
tagGuidWithCloud cloudCode guid
  | cloudCode < 0 || cloudCode >= cloudTagModulus || not (isGuidShaped guid) = guid
  | otherwise = T.dropEnd 1 guid <> T.singleton (intToDigit cloudCode)

cloudCodeOfGuid :: Text -> Maybe Int
cloudCodeOfGuid guid
  | not (isGuidShaped guid) = Nothing
  | otherwise =
    let lastChar = T.last guid
     in if isDigit lastChar then Just (digitToInt lastChar) else Nothing

preferredConnForGuid :: [(Int, Text)] -> Text -> Maybe Text
preferredConnForGuid codes guid = cloudCodeOfGuid guid >>= \c -> lookup c codes
