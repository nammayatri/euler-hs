module EulerHS.KVConnector.Helper.Utils where

import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import EulerHS.Extra.Monitoring.Types
import EulerHS.KVConnector.Types
import qualified EulerHS.Language as L
import EulerHS.Prelude
import EulerHS.SqlDB.Types
import Juspay.Extra.Config (lookupEnvT)

measureLatency :: UTCTime -> IO NominalDiffTime
measureLatency time = do
  currentTime <- getCurrentTime
  return $ diffUTCTime currentTime time

shouldLogLocalLatency :: Bool
shouldLogLocalLatency = fromMaybe False $ readMaybe =<< lookupEnvT @String "LOG_LOCAL_LATENCY"

measureFunctionLatencyAndReturn :: (L.MonadFlow m) => m a -> Text -> Text -> m a
measureFunctionLatencyAndReturn action actionName tableName'
  | not shouldLogLocalLatency = action
  | otherwise = do
      localLatencyId <- L.getOptionLocal LocalLatencyId
      case localLatencyId of
        Just (Just localId) -> do
          startTime <- L.runIO getCurrentTime
          result <- action
          latency <- L.runIO $ measureLatency startTime
          L.logError (localId :: Text) ("Latency for " <> actionName <> " in " <> tableName' <> " is " <> show latency)
          return result
        _ -> action

callKVDBAsync :: (L.MonadFlow m) => m (MeshResult a) -> m (Either DBError b) -> m (MeshResult a, Either DBError b)
callKVDBAsync kvAction dbAction = do
  -- Fork the actions
  kvAwaitable <- L.awaitableFork kvAction
  dbAwaitable <- L.awaitableFork dbAction

  -- Wait for the results
  kvAwaitRes <- L.await Nothing kvAwaitable
  dbAwaitRes <- L.await Nothing dbAwaitable

  -- Handle the results
  case (kvAwaitRes, dbAwaitRes) of
    (Right kvRes, Right dbRes) -> pure (kvRes, dbRes)
    (Left kvErr, Right dbRes) -> pure (Left $ AsyncKVCallFailed (show kvErr), dbRes)
    (Right kvRes, Left dbErr) -> pure (kvRes, Left $ DBError AsyncDBCallFailed (show dbErr))
    (Left kvErr, Left dbErr) -> pure (Left $ AsyncKVCallFailed (show kvErr), Left $ DBError AsyncDBCallFailed (show dbErr))
