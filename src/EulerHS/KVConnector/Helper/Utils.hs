module EulerHS.KVConnector.Helper.Utils where

import           Data.Time.Clock(getCurrentTime, NominalDiffTime, UTCTime, diffUTCTime)
import EulerHS.Prelude

measureLatency :: UTCTime -> IO NominalDiffTime
measureLatency time = do
  currentTime <- getCurrentTime
  return $ diffUTCTime currentTime time