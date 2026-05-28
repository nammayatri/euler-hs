{-# LANGUAGE DeriveAnyClass            #-}
{-# LANGUAGE DeriveGeneric             #-}
{-# LANGUAGE DerivingStrategies        #-}
{-# LANGUAGE DuplicateRecordFields     #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE GADTs                     #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE StandaloneDeriving        #-}
{-# LANGUAGE TypeApplications          #-}
{-# LANGUAGE TypeFamilies              #-}
{-# LANGUAGE UndecidableInstances      #-}
{-# OPTIONS_GHC -Wno-unused-imports -Wno-orphans -Wno-name-shadowing -Wno-missing-signatures -Wno-incomplete-record-updates #-}

module Main (main) where

import           EulerHS.Prelude               hiding (id, show, putStrLn)
import           Prelude                       (Show (show), String, putStrLn)
import qualified Data.Aeson                    as A
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as TE
import           Data.Time                     (UTCTime)
import qualified Data.Vector                   as V
import qualified Database.Beam                 as B
import           Database.Beam.Backend.SQL.Row ()
import qualified Database.Beam.Schema.Tables   as BST
import qualified Database.Beam.Postgres        as BP
import qualified Database.Beam.Postgres        as B (Postgres)
import           Database.Beam.Backend.SQL     (HasSqlValueSyntax (..))
import           Database.Beam.Postgres.Syntax (PgValueSyntax, defaultPgValueSyntax)
import qualified Database.PostgreSQL.Simple    as PGS
import qualified Database.PostgreSQL.Simple.FromField as PGS
import qualified Database.PostgreSQL.Simple.Types as PGS
import           System.Environment            (setEnv)

import qualified EulerHS.CachedSqlDBQuery      as DBQ
import qualified EulerHS.Interpreters          as I
import qualified EulerHS.KVConnector.Metrics   as KVM
import qualified EulerHS.Language              as L
import qualified EulerHS.Runtime               as R
import           EulerHS.Types                 (DBConfig, PoolConfig (..),
                                                PostgresConfig (..),
                                                mkPostgresPoolConfig)
import           Sequelize                     (Clause (..), Term (..), Where,
                                                ModelMeta (..))
import           Sequelize.SQLObject           (ToSQLObject (..),
                                                SQLObject (..))

-- =============================================================================
-- Beam table mirroring atlas_driver_offer_bpp.person
-- =============================================================================

data PersonT f = PersonT
  { id          :: B.C f Text
  , firstName   :: B.C f Text
  , lastName    :: B.C f (Maybe Text)
  , description :: B.C f (Maybe Text)
  , merchantId  :: B.C f Text
  , isNew       :: B.C f Bool
  , createdAt   :: B.C f UTCTime
  , tags        :: B.C f (Maybe [Text])         -- text[]   array
  , mobileHash  :: B.C f (Maybe Text)           -- bytea column; modelled as Maybe Text only for the FALLBACK path (to_jsonb -> "\\xdeadbeef"); normal beam path will type-mismatch (intentional)
  , metadata    :: B.C f (Maybe A.Value)        -- jsonb passthrough
  , pref        :: B.C f (Maybe Text)           -- text storing JSON (mkBeamInstancesForJSON shape)
  } deriving (Generic, B.Beamable)

instance B.Table PersonT where
  data PrimaryKey PersonT f = PersonId (B.C f Text) deriving (Generic, B.Beamable)
  primaryKey = PersonId . Main.id

type Person = PersonT Identity

deriving instance Show Person

-- mkTableInstances-equivalent: genericParseJSON defaultOptions (expects
-- camelCase Haskell field names). Verifies JsonbFallback's key-rewriter
-- converts snake_case columns back to camelCase before decode.
instance A.FromJSON Person where
  parseJSON = A.genericParseJSON A.defaultOptions

instance ModelMeta PersonT where
  modelFieldModification = B.tableModification
    { id          = B.fieldNamed "id"
    , firstName   = B.fieldNamed "first_name"
    , lastName    = B.fieldNamed "last_name"
    , description = B.fieldNamed "description"
    , merchantId  = B.fieldNamed "merchant_id"
    , isNew       = B.fieldNamed "is_new"
    , createdAt   = B.fieldNamed "created_at"
    , tags        = B.fieldNamed "tags"
    , mobileHash  = B.fieldNamed "mobile_hash"
    , metadata    = B.fieldNamed "metadata"
    , pref        = B.fieldNamed "pref"
    }
  modelTableName  = "person"
  modelSchemaName = Just "atlas_driver_offer_bpp"

instance ToSQLObject Bool where
  convertToSQLObject = SQLObjectValue . T.pack . show

-- Orphan instances shared-kernel ships for text[] support
-- (`Kernel.Types.Common` lines 159-170); duplicated here so the test exec
-- can link without depending on shared-kernel.
instance HasSqlValueSyntax PgValueSyntax [Text] where
  sqlValueSyntax xs = sqlValueSyntax (V.fromList xs)

instance PGS.FromField [Text] where
  fromField f mbValue = V.toList <$> PGS.fromField f mbValue

instance B.FromBackendRow B.Postgres [Text]
instance B.HasSqlEqualityCheck B.Postgres [Text]

-- =============================================================================
-- KV metric handler that records jsonb-fallback firings
-- =============================================================================

mkRecordingKvHandler :: IORef Int -> IO KVM.KVMetricHandler
mkRecordingKvHandler ref = do
  base <- KVM.mkKVMetricHandler
  pure base { KVM.jsonbFallback = \(schema, table, err) -> do
                putStrLn $
                  "[live-test] >>> jsonbFallback metric: schema=" <> T.unpack schema
                  <> " table=" <> T.unpack table
                  <> " err=" <> T.unpack (T.take 140 err)
                atomicModifyIORef' ref (\n -> (n + 1, ()))
            }

-- =============================================================================
-- Connection + flow runner
-- =============================================================================

dbConf :: DBConfig BP.Pg
dbConf =
  mkPostgresPoolConfig "jsonb-live-test"
    PostgresConfig
      { connectHost     = "localhost"
      , connectPort     = 5432
      , connectUser     = "postgres"
      , connectPassword = ""
      , connectDatabase = "atlas_driver_offer_bpp_v1"
      }
    PoolConfig
      { stripes            = 1
      , keepAlive          = 10
      , resourcesPerStripe = 4
      }

runFlow :: KVM.KVMetricHandler -> L.Flow a -> IO a
runFlow handler flow =
  R.withFlowRuntime Nothing $ \rt ->
    I.runFlow rt $ do
      _ <- L.initSqlDBConnection dbConf
      L.setOption KVM.KVMetricCfg handler
      flow

-- =============================================================================
-- WHERE merchant_id = 'M1' — matches 2 of 3 seeded rows
-- =============================================================================

whereM1 :: Where BP.Postgres PersonT
whereM1 = [Is merchantId (Eq ("M1" :: Text))]

wherePid :: Text -> Where BP.Postgres PersonT
wherePid v = [Is Main.id (Eq v)]

-- =============================================================================
-- Schema mutation helpers (local sandbox)
-- =============================================================================

runRaw :: Text -> IO ()
runRaw sql = do
  c <- PGS.connectPostgreSQL "host=localhost port=5432 user=postgres dbname=atlas_driver_offer_bpp_v1"
  _ <- PGS.execute_ c (PGS.Query (TE.encodeUtf8 sql))
  PGS.close c

resetSchema :: IO ()
resetSchema = do
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS description varchar(255)"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS last_name   varchar(255)"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS first_name  varchar(255) NOT NULL DEFAULT 'X'"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ALTER COLUMN first_name DROP DEFAULT"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS tags text[]"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS mobile_hash bytea"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS metadata jsonb"
  runRaw "ALTER TABLE atlas_driver_offer_bpp.person ADD COLUMN IF NOT EXISTS pref text"
  runRaw "UPDATE atlas_driver_offer_bpp.person SET tags=ARRAY['vip','english']::text[], mobile_hash=E'\\\\xDEADBEEF'::bytea, metadata='{\"foo\":1}'::jsonb, pref='{\"Sedan\":[]}' WHERE id IN ('p1','p2')"

dropCol :: Text -> IO ()
dropCol col = runRaw ("ALTER TABLE atlas_driver_offer_bpp.person DROP COLUMN " <> col)

-- =============================================================================
-- Test cases
-- =============================================================================

reportEither :: Show e => String -> Either e [Person] -> IO Bool
reportEither label = \case
  Right rs -> do
    putStrLn $ "  " <> label <> ": OK rows=" <> show (length rs)
    forM_ rs $ \p ->
      putStrLn $ "    - " <> T.unpack (Main.id p)
              <> "  desc=" <> show (description p)
              <> "  tags=" <> show (tags p)
              <> "  mobileHash=" <> show (mobileHash p)
              <> "  metadata=" <> show (metadata p)
              <> "  pref=" <> show (pref p)
    pure True
  Left e -> do
    putStrLn $ "  " <> label <> ": ERR " <> show e
    pure False

checkHits :: IORef Int -> Bool -> String -> IO Bool
checkHits ref expected label = do
  n <- readIORef ref
  let ok = (n > 0) == expected
  putStrLn $ "  " <> label <> " jsonbFallback hits=" <> show n
           <> (if ok then " (expected)" else "  *** UNEXPECTED ***")
  pure ok

resetHits :: IORef Int -> IO ()
resetHits ref = atomicModifyIORef' ref (const (0, ()))

main :: IO ()
main = do
  setEnv "KV_METRIC_ENABLED" "True"
  putStrLn "=== Live jsonb-fallback test against atlas_driver_offer_bpp.person ==="
  resetSchema

  hitRef  <- newIORef 0
  handler <- mkRecordingKvHandler hitRef

  putStrLn "\n[A] All columns present, normal findAllSql"
  resetHits hitRef
  rA <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okA1 <- reportEither "A" rA
  okA2 <- checkHits hitRef False "A"

  putStrLn "\n[B] DROP COLUMN description -> expect 42703 -> jsonb fallback"
  dropCol "description"
  resetHits hitRef
  rB <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okB1 <- reportEither "B" rB
  okB2 <- checkHits hitRef True "B"

  putStrLn "\n[C] DROP COLUMN last_name also -> fallback again"
  dropCol "last_name"
  resetHits hitRef
  rC <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okC1 <- reportEither "C" rC
  okC2 <- checkHits hitRef True "C"

  putStrLn "\n[D] DROP COLUMN first_name (required) -> must fail loudly"
  dropCol "first_name"
  resetHits hitRef
  rD <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  let okD = case rD of Left _ -> True; Right _ -> False
  putStrLn $ "  D: " <> (if okD then "OK loud failure" else "*** UNEXPECTED success: " <> show rD <> " ***")

  putStrLn "\n[E] restore schema; findOneSql control"
  resetSchema
  resetHits hitRef
  rE <- runFlow handler $ DBQ.findOneSql dbConf (wherePid "p1")
  let okE1 = case rE of
        Right (Just p) -> Main.id p == "p1"
        _              -> False
  okE2 <- checkHits hitRef False "E"
  putStrLn $ "  E -> " <> show rE

  putStrLn "\n[F] DROP COLUMN description; findOneSql -> expect fallback"
  dropCol "description"
  resetHits hitRef
  rF <- runFlow handler $ DBQ.findOneSql dbConf (wherePid "p1")
  let okF1 = case rF of
        Right (Just _) -> True
        _              -> False
  okF2 <- checkHits hitRef True "F"
  putStrLn $ "  F -> " <> show rF
  resetSchema

  putStrLn "\n[G] DROP COLUMN tags (text[] array) -> expect fallback ok"
  resetSchema
  dropCol "tags"
  resetHits hitRef
  rG <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okG1 <- reportEither "G" rG
  okG2 <- checkHits hitRef True "G"

  putStrLn "\n[H] DROP COLUMN metadata (jsonb Aeson.Value) -> expect fallback ok"
  resetSchema
  dropCol "metadata"
  resetHits hitRef
  rH <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okH1 <- reportEither "H" rH
  okH2 <- checkHits hitRef True "H"

  putStrLn "\n[I] DROP COLUMN mobile_hash (bytea -> JSON string \"\\\\xdeadbeef\") -> expect fallback ok (FromJSON Text accepts)"
  resetSchema
  dropCol "mobile_hash"
  resetHits hitRef
  rI <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okI1 <- reportEither "I" rI
  okI2 <- checkHits hitRef True "I"

  putStrLn "\n[J] DROP COLUMN pref (text storing JSON, mkBeamInstancesForJSON shape) -> Maybe Text accepts ok"
  resetSchema
  dropCol "pref"
  resetHits hitRef
  rJ <- runFlow handler $ DBQ.findAllSql dbConf whereM1
  okJ1 <- reportEither "J" rJ
  okJ2 <- checkHits hitRef True "J"

  resetSchema

  let results = [okA1, okA2, okB1, okB2, okC1, okC2, okD, okE1, okE2, okF1, okF2,
                 okG1, okG2, okH1, okH2, okI1, okI2, okJ1, okJ2]
      passed = length (filter (== True) results)
  putStrLn $ "\n=== Summary: " <> show passed <> "/" <> show (length results) <> " passed ==="
  unless (and results) exitFailure
