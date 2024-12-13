module EulerHS.KVConnector.Helper.Utils where

import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import EulerHS.Extra.Monitoring.Types
import qualified EulerHS.Language as L
import EulerHS.Prelude
import Juspay.Extra.Config (lookupEnvT)

measureLatency :: UTCTime -> IO NominalDiffTime
measureLatency time = do
  currentTime <- getCurrentTime
  return $ diffUTCTime currentTime time

shouldLogLocalLatency :: Bool
shouldLogLocalLatency = fromMaybe False $ readMaybe =<< lookupEnvT @String "LOG_LOCAL_LATENCY"

measureFunctionLatencyAndReturn :: (L.MonadFlow m) => m a -> Text -> Text -> m a
measureFunctionLatencyAndReturn action actionName tableName =
  if shouldLogLocalLatency
    then do
      localLatencyId <- L.getOptionLocal LocalLatencyId
      case localLatencyId of
        Nothing -> do
          action
        Just localId -> do
          startTime <- L.runIO getCurrentTime
          result <- action
          latency <- L.runIO $ measureLatency startTime
          L.logInfo localId ("Latency for " <> actionName <> " in " <> tableName <> " is " <> show latency)
          return result
    else do
      action
