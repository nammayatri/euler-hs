{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-name-shadowing #-}
{-# OPTIONS_GHC -Wno-unused-matches #-}

-- | Benchmark module measuring where latency comes from in KV findAll.
--
-- See @bench/Main.hs@ for the executable entry point.  The module
-- intentionally lives under the library so the Template-Haskell splices that
-- wire up the @bench_table@ row compile alongside @EulerHS.KVConnector.*@.
--
-- The bench populates @bench_table@ with a mix of rows stored live in Redis
-- (via 'createWithKVConnector') and rows stored in Postgres only, then runs
-- five different findAll strategies and prints timing percentiles.
module EulerHS.Test (main) where

import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BSL
import qualified Data.List as DL
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import qualified Database.Beam as B
import Database.Beam.Backend (HasSqlValueSyntax (..), autoSqlValueSyntax)
import Database.Beam.Backend.SQL (BeamSqlBackend)
import qualified Database.Beam.Postgres as BP
import qualified Database.PostgreSQL.Simple as PGS
import EulerHS.CachedSqlDBQuery (findAllSql, runQuery)
import qualified EulerHS.Interpreters as I
import Control.Exception (ErrorCall (..))
import qualified EulerHS.KVConnector.Flow as KVFlow
import EulerHS.KVConnector.Types
  ( MeshConfig (..),
    MeshError,
    MeshResult,
  )
import qualified EulerHS.KVConnector.Utils as KVUtils
import EulerHS.KVConnector.UtilsTH (enableKVPG, mkTableInstances)
import qualified EulerHS.Language as L
import EulerHS.Language (cancelAwaitable)
import EulerHS.Prelude hiding (id)
import qualified EulerHS.Runtime as R
import qualified EulerHS.SqlDB.Language as DB
import qualified EulerHS.Types as ET
import Sequelize (Clause (..), Term (..), Where)
import qualified Sequelize as S

----------------------------------------------------------------------
-- Beam table
----------------------------------------------------------------------

data BenchTableT f = BenchTableT
  { id :: B.C f Text,
    merchantId :: B.C f Text,
    status :: B.C f Text,
    value :: B.C f Int,
    createdAt :: B.C f UTCTime
  }
  deriving (Generic, B.Beamable)

instance B.Table BenchTableT where
  data PrimaryKey BenchTableT f = BenchTableId (B.C f Text)
    deriving (Generic, B.Beamable)
  primaryKey = BenchTableId . id

type BenchTable = BenchTableT Identity

-- TH-generated KV connector instances + table metadata
$(enableKVPG ''BenchTableT ['id] [])

$(mkTableInstances ''BenchTableT "bench_table" "public")

----------------------------------------------------------------------
-- Config
----------------------------------------------------------------------

benchMeshCfg :: MeshConfig
benchMeshCfg =
  MeshConfig
    { meshEnabled = True,
      kvHardKilled = False,
      kvRedis = "bench_redis",
      kvRedisSecondary = "bench_redis",
      secondaryRedisEnabled = False,
      tableShardModRange = (0, 9),
      redisKeyPrefix = "bench:",
      redisTtl = 3600,
      forceDrainToDB = False,
      cerealEnabled = True,
      memcacheEnabled = False,
      meshDBName = "bench_kv",
      ecRedisDBStream = "bench_stream"
    }

-- Variant with KV disabled so findAllWithKVConnector goes straight to DB
benchMeshCfgKvDisabled :: MeshConfig
benchMeshCfgKvDisabled = benchMeshCfg {kvHardKilled = True}

pgCfg :: ET.PostgresConfig
pgCfg =
  ET.PostgresConfig
    { connectHost = "localhost",
      connectPort = 5432,
      connectUser = "vijaygupta",
      connectPassword = "",
      connectDatabase = "bench_kv"
    }

mkPgDbCfg :: ET.PostgresConfig -> ET.DBConfig BP.Pg
mkPgDbCfg = ET.mkPostgresPoolConfig "bench_kv_db" `flip` poolCfg
  where
    poolCfg =
      ET.PoolConfig
        { stripes = 1,
          keepAlive = 10,
          resourcesPerStripe = 8
        }

kvRedisConfig :: ET.KVDBConfig
kvRedisConfig = ET.mkKVDBClusterConfig "bench_redis" redisCfg
  where
    redisCfg =
      ET.RedisConfig
        { connectHost = "127.0.0.1",
          connectPort = 7100,
          connectAuth = Nothing,
          connectDatabase = 0,
          connectReadOnly = False,
          connectMaxConnections = 16,
          connectMaxIdleTime = 30,
          connectTimeout = Nothing
        }

dbConf :: ET.DBConfig BP.Pg
dbConf = mkPgDbCfg pgCfg

----------------------------------------------------------------------
-- Table setup + seeding
----------------------------------------------------------------------

createTable :: L.Flow ()
createTable = do
  -- We execute raw DDL via postgresql-simple since Beam doesn't expose CREATE TABLE.
  -- Grab a connection directly via initSqlDBConnection and use it with a custom action.
  econn <- L.getOrInitSqlConn dbConf
  case econn of
    Left e -> L.throwException $ ErrorCall $ "DB connect fail: " <> show e
    Right _ -> do
      let ddl =
            "CREATE TABLE IF NOT EXISTS public.bench_table ("
              <> "id TEXT PRIMARY KEY,"
              <> "merchant_id TEXT NOT NULL,"
              <> "status TEXT NOT NULL,"
              <> "value INTEGER NOT NULL,"
              <> "created_at TIMESTAMPTZ NOT NULL"
              <> ")"
      _ <- L.runIO $ withRawPg $ \c -> PGS.execute_ c ddl
      pure ()

truncateTable :: L.Flow ()
truncateTable = do
  _ <- L.runIO $ withRawPg $ \c -> PGS.execute_ c "TRUNCATE TABLE public.bench_table"
  pure ()

-- | Connect directly to Postgres (bypassing EulerHS pool) for DDL.
withRawPg :: (PGS.Connection -> IO a) -> IO a
withRawPg act = do
  c <-
    PGS.connect
      PGS.defaultConnectInfo
        { PGS.connectHost = ET.connectHost (pgCfg :: ET.PostgresConfig),
          PGS.connectPort = ET.connectPort (pgCfg :: ET.PostgresConfig),
          PGS.connectUser = ET.connectUser (pgCfg :: ET.PostgresConfig),
          PGS.connectPassword = ET.connectPassword (pgCfg :: ET.PostgresConfig),
          PGS.connectDatabase = ET.connectDatabase (pgCfg :: ET.PostgresConfig)
        }
  r <- act c
  PGS.close c
  pure r

mkRow :: Int -> UTCTime -> BenchTable
mkRow i now =
  BenchTableT
    { id = "bench-" <> T.pack (show i),
      merchantId = "merchant-" <> T.pack (show (i `mod` 5)),
      status = if even i then "ACTIVE" else "INACTIVE",
      value = i,
      createdAt = now
    }

-- | Seed N rows; the first @n * kvFraction@ go through KV (live in Redis + WAL),
-- the rest are inserted directly into Postgres (so they're DB-only from KV's POV).
-- Returns the full list of seeded primary-key ids (both KV and DB-only rows).
-- When @alsoAllInDb=True@ the KV-live rows are ALSO inserted into Postgres —
-- models the realistic "drainer caught up" state where every row exists in
-- both stores.
seed :: Int -> Double -> Bool -> L.Flow [Text]
seed n kvFrac alsoAllInDb = do
  now <- L.runIO getCurrentTime
  let kvCount = floor (fromIntegral n * kvFrac :: Double)
  let (kvIdxs, dbOnlyIdxs) = splitAt kvCount [1 .. n]
  -- 1. KV-stored rows
  forM_ kvIdxs $ \i -> do
    let row = mkRow i now
    res <- KVFlow.createWithKVConnector dbConf benchMeshCfg row
    case res of
      Right _ -> pure ()
      Left e -> L.logErrorT "seed" $ "createKV failed: " <> T.pack (show e)
  -- 2. DB rows — DB-only indices, plus (optionally) ALL indices if we're
  -- simulating drainer-caught-up state.
  let dbIdxs = if alsoAllInDb then [1 .. n] else dbOnlyIdxs
  unless (null dbIdxs) $ do
    let rows = fmap (`mkRow` now) dbIdxs
    res <-
      runQuery dbConf $
        DB.insertRows $
          B.insert S.modelTableEntity (B.insertValues rows)
    case res of
      Right _ -> pure ()
      Left e -> L.logErrorT "seed" $ "db insert failed: " <> T.pack (show e)
  pure $ fmap (\i -> "bench-" <> T.pack (show i)) [1 .. n]

----------------------------------------------------------------------
-- The five findAll modes
----------------------------------------------------------------------

type FindAllWhere = Where BP.Postgres BenchTableT

-- | Build the where-clause used by every mode: @id IN (requestedIds)@.
-- This mirrors 'getDriverInfosWithCond' in the driver app, which filters by
-- @driverId IN (...)@ on the primary key.  Because @id@ is the declared PK in
-- 'enableKVPG', 'redisFindAll' resolves each value directly to a Redis key and
-- the KV path actually runs (vs. the old @merchantId Eq _@ clause which was a
-- non-key field and short-circuited KV to empty).
whereClauseForIds :: [Text] -> FindAllWhere
whereClauseForIds ids = [S.Is id (S.In ids)]

-- | 1. Pure DB: hit Postgres only, skip KV entirely.
pureDb :: [Text] -> L.Flow (MeshResult [BenchTable])
pureDb = KVFlow.findAllWithKVConnector dbConf benchMeshCfgKvDisabled . whereClauseForIds

-- | 2. Pure pipelined KV: skip SQL leg, use redisFindAll directly.
pureKvPipelined :: [Text] -> L.Flow (MeshResult [BenchTable])
pureKvPipelined requestedIds = do
  res <- KVFlow.redisFindAll benchMeshCfg (whereClauseForIds requestedIds)
  pure $ fmap (\(live, _dead) -> live) res

-- | 3. Current production path (async KV + SQL parallel leg + merge).
kvAsyncCurrent :: [Text] -> L.Flow (MeshResult [BenchTable])
kvAsyncCurrent = KVFlow.findAllWithKVConnector dbConf benchMeshCfg . whereClauseForIds

-- | 4. Early-return KV: run redisFindAll first; if some PKs are missing from
--     the KV result, fetch just the missing ones from SQL and merge.  This is
--     the proposed optimisation over the current always-parallel path.
kvEarlyReturn :: [Text] -> L.Flow (MeshResult [BenchTable])
kvEarlyReturn requestedIds = do
  kvRes <- KVFlow.redisFindAll benchMeshCfg (whereClauseForIds requestedIds)
  case kvRes of
    Left e -> pure (Left e)
    Right (live, _dead) -> do
      let hitSet = Set.fromList (fmap (id :: BenchTable -> Text) live)
          missing = filter (\pk -> not (Set.member pk hitSet)) requestedIds
      if null missing
        then pure (Right live)
        else do
          sqlRes <-
            KVFlow.findAllWithKVConnector
              dbConf
              benchMeshCfgKvDisabled
              (whereClauseForIds missing)
          case sqlRes of
            Left e -> pure (Left e)
            Right missingRows -> pure (Right (live ++ missingRows))

-- | 5. Same as 3 but with secondary Redis explicitly disabled (already the
--     default here; kept as a distinct mode for symmetry with the prod matrix).
kvNoSecondary :: [Text] -> L.Flow (MeshResult [BenchTable])
kvNoSecondary =
  KVFlow.findAllWithKVConnector dbConf benchMeshCfg {secondaryRedisEnabled = False}
    . whereClauseForIds

-- | 6. Race with early cancel: fork both KV and SQL (mirrors 'callKVDBAsync'),
--     wait for KV first. If KV has all requested PKs, cancel the SQL fork and
--     return immediately. Else await SQL and merge.
--
--     Thanks to 'cancelAwaitable' (added to EulerHS.Common), the SQL fork is
--     actually killed — no pool backpressure from zombie queries.
--
--     We fork 'findAllSql' (not 'findAllWithKVConnector dbConf kvHardKilled')
--     to match the exact workload 'kvAsyncCurrent' submits, so the comparison
--     is apples-to-apples.
kvRaceEarlyCancel :: [Text] -> L.Flow (MeshResult [BenchTable])
kvRaceEarlyCancel requestedIds = do
  let w = whereClauseForIds requestedIds
  sqlAwait <- L.awaitableFork $ findAllSql dbConf w
  kvAwait  <- L.awaitableFork $ KVFlow.redisFindAll benchMeshCfg w
  eKv <- L.await Nothing kvAwait
  case eKv of
    Left _awaitErr -> awaitSqlOnly sqlAwait
    Right (Left _meshErr) -> awaitSqlOnly sqlAwait
    Right (Right (live, _dead)) -> do
      let kvPks  = Set.fromList (fmap (id :: BenchTable -> Text) live)
          allHit = DL.all (`Set.member` kvPks) requestedIds
      if allHit
        then do
          L.runIO $ cancelAwaitable sqlAwait   -- actually kill the SQL thread
          pure (Right live)
        else awaitSql sqlAwait (Right live)
  where
    awaitSqlOnly sqlAwait = do
      eSql <- L.await Nothing sqlAwait
      pure $ case eSql of
        Left _ -> Right []
        Right (Left _dbErr) -> Right []
        Right (Right rows) -> Right rows

    -- Wait for the forked SQL, merge with whatever we already have from KV.
    -- NB: sqlAwait here is 'findAllSql' whose payload is 'Either DBError [row]',
    -- not a 'MeshResult'. Hence the pattern shapes differ from 'awaitSqlOnly'.
    awaitSql sqlAwait (Right liveSoFar) = do
      eRes <- L.await Nothing sqlAwait
      case eRes of
        Left _awaitErr -> pure (Right liveSoFar)
        Right (Left _dbErr) -> pure (Right liveSoFar)
        Right (Right sqlRows) -> do
          let kvPks = Set.fromList (fmap (id :: BenchTable -> Text) liveSoFar)
              extraSql = filter (\r -> not (Set.member ((id :: BenchTable -> Text) r) kvPks)) sqlRows
          pure (Right (liveSoFar ++ extraSql))
    awaitSql _ (Left e) = pure (Left e)

-- | 7. Optimistic KV: fire SQL as safety net, but if KV returns *any* rows,
--     trust it and cancel SQL. Only wait for SQL when KV is completely empty.
--     Correctness caveat: if KV returned partial results (some PKs missing),
--     we silently return the partial set. Safe when you trust KV to either
--     have everything relevant or nothing (e.g. querying recently-updated
--     entities whose KV entries haven't expired).
kvOptimistic :: [Text] -> L.Flow (MeshResult [BenchTable])
kvOptimistic requestedIds = do
  let w = whereClauseForIds requestedIds
  sqlAwait <- L.awaitableFork $ findAllSql dbConf w
  kvRes <- KVFlow.redisFindAll benchMeshCfg w
  case kvRes of
    Right (live, _dead) | not (null live) -> do
      L.runIO $ cancelAwaitable sqlAwait
      pure (Right live)
    _ -> do
      eSql <- L.await Nothing sqlAwait
      pure $ case eSql of
        Left _ -> Right []
        Right (Left _dbErr) -> Right []
        Right (Right rows) -> Right rows

----------------------------------------------------------------------
-- Timing harness
----------------------------------------------------------------------

-- | Run an action, return (result, elapsed in microseconds).
timed :: L.Flow a -> L.Flow (a, Double)
timed act = do
  t0 <- L.runIO getCurrentTime
  !r <- act
  t1 <- L.runIO getCurrentTime
  let us = realToFrac (diffUTCTime t1 t0) * 1e6 :: Double
  pure (r, us)

percentile :: Double -> [Double] -> Double
percentile _ [] = 0
percentile p xs =
  let sorted = DL.sort xs
      n = length sorted
      idx = min (n - 1) (max 0 (floor (p * fromIntegral n :: Double)))
   in sorted DL.!! idx

meanD :: [Double] -> Double
meanD [] = 0
meanD xs = sum xs / fromIntegral (length xs)

-- | Run a mode N times and collect per-call latency in microseconds.
runMode ::
  Text ->
  Int ->
  Double ->
  Int ->
  ([Text] -> L.Flow (MeshResult [BenchTable])) ->
  [Text] ->
  L.Flow ()
runMode modeName n kvFrac reps act ids = do
  -- Warm-up (ignored)
  _ <- act ids
  samples <- replicateM reps $ do
    (_, us) <- timed (act ids)
    pure us
  let p50 = percentile 0.50 samples
      p95 = percentile 0.95 samples
      p99 = percentile 0.99 samples
      mu = meanD samples
  L.runIO $
    putStrLn @String $
      T.unpack $
        T.intercalate
          "\t"
          [ modeName,
            T.pack (show n),
            T.pack (show kvFrac),
            fmt p50,
            fmt p95,
            fmt p99,
            fmt mu
          ]
  where
    fmt d = T.pack $ show (round d :: Int)

----------------------------------------------------------------------
-- Scenarios
----------------------------------------------------------------------

-- | Scenario: (n, kvFrac, alsoAllInDb).  @alsoAllInDb=True@ duplicates KV-live
-- rows into Postgres — the realistic "drainer caught up" production state.
scenarios :: [(Int, Double, Bool)]
scenarios =
  [ (100, 0.5, False),
    (500, 0.5, False),
    (1000, 0.5, False),
    (1000, 0.0, False),
    (1000, 1.0, False),
    -- Production-like: all rows in DB *and* KV-hot portion also in Redis
    (1000, 1.0, True),    -- KV has everything AND DB has everything (best case for cancel)
    (1000, 0.5, True)     -- Half in KV, everything in DB
  ]

-- | Run all six modes for a given scenario.
-- Fetches *all* seeded PKs via @id IN [pks]@, matching
-- 'getDriverInfosWithCond' — Se.Is BeamDI.driverId (Se.In personsKeys).
runScenario :: Int -> Double -> Bool -> Int -> L.Flow ()
runScenario n kvFrac alsoAllInDb reps = do
  truncateTable
  ids <- seed n kvFrac alsoAllInDb
  L.runIO $ putStrLn @String $
    "# scenario n=" <> show n <> " kvFrac=" <> show kvFrac
      <> " alsoAllInDb=" <> show alsoAllInDb <> " reps=" <> show reps
  runMode "pureDb" n kvFrac reps pureDb ids
  runMode "pureKvPipelined" n kvFrac reps pureKvPipelined ids
  runMode "kvAsyncCurrent" n kvFrac reps kvAsyncCurrent ids
  runMode "kvEarlyReturn" n kvFrac reps kvEarlyReturn ids
  runMode "kvNoSecondary" n kvFrac reps kvNoSecondary ids
  runMode "kvRaceEarlyCancel" n kvFrac reps kvRaceEarlyCancel ids
  runMode "kvOptimistic" n kvFrac reps kvOptimistic ids

----------------------------------------------------------------------
-- Main
----------------------------------------------------------------------

prepareFlow :: L.Flow ()
prepareFlow = do
  -- Initialise Postgres + Redis connections
  ePool <- L.initSqlDBConnection dbConf
  L.throwOnFailedWithLog ePool L.SqlDBConnectionFailedException "Postgres connect failed"
  eKv <- L.initKVDBConnection kvRedisConfig
  L.throwOnFailedWithLog eKv L.KVDBConnectionFailedException "Redis cluster connect failed"
  createTable

main :: IO ()
main = do
  R.withFlowRuntime Nothing $ \rt -> do
    I.runFlow rt $ do
      prepareFlow
      L.runIO $ putStrLn @String "mode\tn\tkvFrac\tp50_us\tp95_us\tp99_us\tmean_us"
      forM_ scenarios $ \(n, kvFrac, alsoAllInDb) -> runScenario n kvFrac alsoAllInDb 100
