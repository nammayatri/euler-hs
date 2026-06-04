{-# LANGUAGE ScopedTypeVariables #-}

module SQLDB.TestData.Connections where

import EulerHS.Interpreters
import qualified EulerHS.Language as L
import EulerHS.Prelude
import EulerHS.Runtime (withFlowRuntime)
import qualified EulerHS.Runtime as R
import qualified EulerHS.Types as T

connectOrFail :: T.DBConfig beM -> L.Flow (T.SqlConn beM)
connectOrFail cfg =
  L.initSqlDBConnection cfg >>= \case
    Left e -> error $ show e -- L.throwException $ toException $ show e
    Right conn -> pure conn

testDBName :: String
testDBName = "./test/language/EulerHS/TestData/test.db"

-- | Schema for the throwaway SQLite test DB. Built fresh from SQL on every run
--   (see 'prepareTestDB') instead of copying a checked-in binary @.db.template@,
--   so the fixture is text, deterministic, and survives a clean checkout.
testDBSchema :: String
testDBSchema =
  "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, first_name VARCHAR NOT NULL, last_name VARCHAR NOT NULL);"

rmTestDB :: L.Flow ()
rmTestDB = void $ L.runSysCmd $ "rm -f " <> testDBName

prepareTestDB :: (T.DBConfig beM -> L.Flow ()) -> T.DBConfig beM -> L.Flow ()
prepareTestDB insertValues cfg = do
  rmTestDB
  void $ L.runSysCmd $ "sqlite3 " <> testDBName <> " " <> show testDBSchema
  insertValues cfg

withEmptyDB :: (T.DBConfig beM -> L.Flow ()) -> T.DBConfig beM -> (R.FlowRuntime -> IO ()) -> IO ()
withEmptyDB insertValues cfg act =
  withFlowRuntime
    Nothing
    ( \rt -> do
        try (runFlow rt $ prepareTestDB insertValues cfg) >>= \case
          Left (e :: SomeException) ->
            runFlow rt rmTestDB
              `finally` error ("Preparing test values failed: " <> show e)
          Right _ -> act rt `finally` runFlow rt rmTestDB
    )
