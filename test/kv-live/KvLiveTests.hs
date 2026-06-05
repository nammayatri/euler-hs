{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-name-shadowing -Wno-type-defaults #-}

-- =============================================================================
-- kv-live-test — a full-surface, live, end-to-end test of the EulerHS KV
-- connector against a real Postgres + two Redis instances.
--
-- The table under test (Schema.KvLiveT) is defined the SAME way Namma Yatri
-- defines its KV tables: a Beam record + the `enableKVPGWithSortKey` splice
-- (see KvConnectorTH). Every public connector function in
-- EulerHS.KVConnector.Flow is exercised, asserting the RETURNED value, the
-- resulting Redis index state (SET membership / tombstones / index moves), the
-- DB-fallback path, multi-cloud union/dedup, and the error results.
--
-- Goal: if this is green for a table, that table's KV behaviour is proven —
-- no UAT round-trip needed.
--
-- Infra (same as sorted-set-live):
--   postgres on 5432, database euler_sorted_test  (table kv_live is auto-created)
--   redis-server --port 6379   (primary cloud)
--   redis-server --port 6380   (secondary cloud)
--   cabal run euler-hs:exe:kv-live-test
-- =============================================================================
module KvLiveTests (runKvLiveTests, runRecacheTests) where

import qualified Data.Aeson as A
import Data.Aeson.Types (Parser, parseMaybe)
import qualified Data.ByteString as BS
import qualified Data.Text as T
import Data.Time (UTCTime (..), addUTCTime, fromGregorian, secondsToDiffTime)
import qualified Database.Beam.Postgres as BP
import qualified Database.PostgreSQL.Simple as PGS
import qualified EulerHS.Interpreters as I
import EulerHS.KVConnector.Flow
  ( createWithKVConnector,
    createWoReturingKVConnector,
    deleteAllReturningWithKVConnector,
    deleteReturningWithKVConnector,
    deleteWithKVConnector,
    findAllFromKvRedis,
    findAllWithKVAndConditionalDBInternal,
    findAllWithKVConnector,
    findAllWithOptionsKVConnector,
    findAllWithOptionsKVConnector',
    findOneFromKvRedis,
    findWithKVConnector,
    updateAllReturningWithKVConnector,
    updateAllWithKVConnector,
    updateWithKVConnector,
    updateWoReturningWithKVConnector,
  )
import EulerHS.KVConnector.Types (MeshConfig (..), MeshMeta (..), MeshResult, TermWrap)
import EulerHS.KVConnector.Utils (getPKeyWithShard)
import qualified EulerHS.Language as L
import EulerHS.Prelude hiding (id, note, putStrLn, show)
import qualified EulerHS.Runtime as R
import EulerHS.Types
  ( DBConfig,
    PoolConfig (..),
    PostgresConfig (..),
    mkPostgresPoolConfig,
  )
import qualified EulerHS.Types as T
import Schema
import Sequelize (Clause (..), OrderBy (..), Set (..), Term (..))
import Prelude (Show (show), putStrLn)

-- =============================================================================
-- Config & infra
-- =============================================================================

dbConf :: DBConfig BP.Pg
dbConf =
  mkPostgresPoolConfig
    "kv-live"
    PostgresConfig
      { connectHost = "localhost",
        connectPort = 5432,
        connectUser = "postgres",
        connectPassword = "",
        connectDatabase = "euler_sorted_test"
      }
    PoolConfig {stripes = 1, keepAlive = 10, resourcesPerStripe = 4}

connName, connNameSecondary :: Text
connName = "KVRedis"
connNameSecondary = "KVRedisSecondary"

redisCfg :: T.KVDBConfig
redisCfg = T.mkKVDBConfig connName T.defaultKVDBConnConfig

redisCfgSecondary :: T.KVDBConfig
redisCfgSecondary = T.mkKVDBConfig connNameSecondary (T.defaultKVDBConnConfig {T.connectPort = 6380})

-- Primary-only (single cloud) config — the common case.
meshCfg :: MeshConfig
meshCfg =
  MeshConfig
    { meshEnabled = True,
      cerealEnabled = False,
      memcacheEnabled = False,
      meshDBName = "ECRDB",
      ecRedisDBStream = "db-sync-stream",
      kvRedis = connName,
      kvRedisSecondary = connName,
      secondaryRedisEnabled = False,
      redisTtl = 3600,
      kvHardKilled = False,
      tableShardModRange = (0, 128),
      redisKeyPrefix = "",
      forceDrainToDB = False,
      disableSecondaryKeys = [],
      forceUnorderedSecondaryKeys = False,
      inOrderedReadStrategy = Nothing
    }

-- Treat the secondary Redis as this pod's primary (used to seed the "other cloud").
cfgSecondaryAsPrimary :: MeshConfig
cfgSecondaryAsPrimary = meshCfg {kvRedis = connNameSecondary}

-- Real multi-cloud: primary = 6379, secondary = 6380, cross-cloud reads on.
cfgMultiCloud :: MeshConfig
cfgMultiCloud = meshCfg {kvRedis = connName, kvRedisSecondary = connNameSecondary, secondaryRedisEnabled = True}

baseTime :: UTCTime
baseTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 0)

-- id  group  status  cnt  note  secondsOffset
mkRow :: Text -> Text -> Text -> Int -> Maybe Text -> Integer -> KvLive
mkRow i g s c n secs =
  KvLiveT
    { id = i,
      groupId = g,
      status = s,
      cnt = c,
      note = n,
      updatedAt = addUTCTime (fromIntegral secs) baseTime,
      payload = "p-" <> i
    }

enc :: Text -> ByteString
enc = encodeUtf8

runFlow :: L.Flow a -> IO a
runFlow flow =
  R.withFlowRuntime Nothing $ \rt ->
    I.runFlow rt $ do
      _ <- L.initSqlDBConnection dbConf
      _ <- L.initKVDBConnection redisCfg
      _ <- L.initKVDBConnection redisCfgSecondary
      flow

runRaw :: Text -> IO ()
runRaw sql = do
  c <- PGS.connectPostgreSQL "host=localhost port=5432 user=postgres dbname=euler_sorted_test"
  _ <- PGS.execute_ c (fromString (T.unpack sql))
  PGS.close c

ensureTable :: IO ()
ensureTable =
  runRaw $
    "CREATE TABLE IF NOT EXISTS kv_live "
      <> "(id text PRIMARY KEY, group_id text NOT NULL, status text NOT NULL, "
      <> "cnt int NOT NULL, note text, updated_at timestamptz NOT NULL, payload text NOT NULL)"

cleanPg :: IO ()
cleanPg = runRaw "DELETE FROM kv_live"

-- Insert a row directly into Postgres (NOT via KV) — to exercise the DB-fallback path.
insertDbRow :: KvLive -> IO ()
insertDbRow r = do
  c <- PGS.connectPostgreSQL "host=localhost port=5432 user=postgres dbname=euler_sorted_test"
  _ <-
    PGS.execute
      c
      "INSERT INTO kv_live (id,group_id,status,cnt,note,updated_at,payload) VALUES (?,?,?,?,?,?,?)"
      (id r, groupId r, status r, cnt r, note r, updatedAt r, payload r)
  PGS.close c

-- Hermetic: drop every key this test owns on BOTH clouds. Only the test's own
-- keyspace (kvLive_*, its Z:: sorted-index mirror) is touched — never a global FLUSH.
flushTestKeys :: L.Flow ()
flushTestKeys =
  forM_ [connName, connNameSecondary] $ \conn ->
    forM_ ["kvLive*", "Z::kvLive*"] $ \pat -> do
      eks <- L.runKVDB conn $ L.rawRequest ["KEYS", enc pat]
      case eks of
        Right (ks :: [ByteString]) -> unless (null ks) . void . L.runKVDB conn $ L.del ks
        Left _ -> pure ()

-- =============================================================================
-- Redis introspection helpers (raw, so assertions are about the ACTUAL stored state)
-- =============================================================================

-- The member stored in the secondary SET/ZSET (pkey incl. shard hash-tag).
pkOf :: KvLive -> ByteString
pkOf r = enc $ getPKeyWithShard r meshCfg.redisKeyPrefix meshCfg.tableShardModRange

-- Secondary SET keys, constructed exactly as the connector does: <table>_<field>_<value>.
groupSetKey :: Text -> Text
groupSetKey g = "kvLive_groupId_" <> g

sIsMember :: Text -> Text -> ByteString -> L.Flow Bool
sIsMember conn key member = do
  r <- L.runKVDB conn $ L.rawRequest ["SISMEMBER", enc key, member]
  pure $ case (r :: Either T.KVDBReply Integer) of
    Right 1 -> True
    _ -> False

-- Raw GET of a pkey's stored bytes (Nothing if the key is absent / hard-deleted).
redisGetRaw :: Text -> ByteString -> L.Flow (Maybe ByteString)
redisGetRaw conn key = do
  r <- L.runKVDB conn $ L.rawRequest ["GET", key]
  pure $ case (r :: Either T.KVDBReply (Maybe ByteString)) of
    Right mb -> mb
    Left _ -> Nothing

-- =============================================================================
-- Assertion helper
-- =============================================================================

check :: String -> Bool -> IO Bool
check label ok = do
  putStrLn $ "  " <> (if ok then "[PASS] " else "[FAIL] ") <> label
  pure ok

-- Compare only the Right side (MeshError has no Eq instance). Polymorphic in the
-- error type so it also works for the Either String produced by 'sortedIds'.
(=?=) :: Eq a => Either e a -> a -> Bool
Right y =?= x = y == x
Left _ =?= _ = False

infix 4 =?=

ids :: [KvLive] -> [Text]
ids = map id

-- pull the id list out of a MeshResult, sorted (order-insensitive comparisons)
sortedIds :: MeshResult [KvLive] -> Either String [Text]
sortedIds (Right rs) = Right (sort (ids rs))
sortedIds (Left e) = Left (show e)

-- =============================================================================
-- main
-- =============================================================================

-- | Run the full KV-connector surface against live PG + Redis. Returns True iff
--   every assertion passed. Sets up its own table and cleans its own PG rows +
--   Redis keys, so the caller need only ensure the servers are up.
runKvLiveTests :: IO Bool
runKvLiveTests = do
  putStrLn "\n############################################################"
  putStrLn "#  kv-live — full KV connector surface (live PG + Redis)"
  putStrLn "############################################################"
  ensureTable
  cleanPg
  runFlow flushTestKeys

  results <-
    sequence
      [ tCreateReturning,
        tCreateWoReturning,
        tFindByPk,
        tFindBySecondary,
        tFindBySecondaryStatus,
        tFindMiss,
        tFindDbFallback,
        tFindOneFromKvRedis,
        tFindAll,
        tFindAllEmpty,
        tFindAllUnionDedup,
        tFindAllFromKvRedis,
        tFindAllConditionalDb,
        tFindOneHalfDrained,
        tFindAllHalfDrained,
        tFindAllInHalfDrained,
        tFindAllWithOptions,
        tFindAllWithOptionsNoOrder,
        tResidualFilter,
        tUpdateReturning,
        tUpdateWoReturning,
        tUpdateMovesSecondaryIndex,
        tUpdateAdditiveMerge,
        tUpdateAllReturning,
        tUpdateAll,
        tDeleteTombstone,
        tDeleteReturning,
        tDeleteAllReturning,
        tMultiCloudFindPrimaryWins,
        tMultiCloudDedup,
        tFindOneOfMany,
        tMultiCloudSecondaryOnlyRead,
        tDeleteTombstoneMarker,
        tUpdateMaybeToNothing,
        tHardKilled,
        tParseClauses,
        tJsonbFallbackOnSchemaDrift
      ]

  let passed = length (filter (== True) results)
      total = length results
  putStrLn $ "\n=== kv-live: " <> show passed <> "/" <> show total <> " passed ==="
  pure (passed == total)

-- =============================================================================
-- CREATE
-- =============================================================================

tCreateReturning :: IO Bool
tCreateReturning = do
  putStrLn "\n[1] createWithKVConnector — returns the created row, and it is findable + indexed"
  let row = mkRow "c1" "gC" "active" 1 (Just "n1") 100
  (res, found, inSet) <- runFlow $ do
    r <- createWithKVConnector dbConf meshCfg row
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("c1" :: Text))]
    m <- sIsMember connName (groupSetKey "gC") (pkOf row)
    pure (r, f, m)
  ok1 <- check "returns Right row == input" $ res =?= row
  ok2 <- check "findWithKVConnector finds it" $ found =?= (Just row)
  ok3 <- check "secondary SET has the pkey member" inSet
  pure (ok1 && ok2 && ok3)

tCreateWoReturning :: IO Bool
tCreateWoReturning = do
  putStrLn "\n[2] createWoReturingKVConnector — returns (), row is still written + findable"
  let row = mkRow "c2" "gC2" "active" 2 Nothing 100
  (res, found) <- runFlow $ do
    r <- createWoReturingKVConnector dbConf meshCfg row
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("c2" :: Text))]
    pure (r, f)
  ok1 <- check "returns Right ()" $ res =?= ()
  ok2 <- check "row is findable (note=Nothing round-trips)" $ found =?= (Just row)
  pure (ok1 && ok2)

-- =============================================================================
-- READ — find one
-- =============================================================================

tFindByPk :: IO Bool
tFindByPk = do
  putStrLn "\n[3] findWithKVConnector by primary key (Redis hit)"
  let row = mkRow "f3" "gF3" "active" 3 Nothing 100
  found <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    findWithKVConnector dbConf meshCfg [Is id (Eq ("f3" :: Text))]
  check "id=f3 ⇒ Just f3" $ found =?= (Just row)

tFindBySecondary :: IO Bool
tFindBySecondary = do
  putStrLn "\n[4] findWithKVConnector by secondary index (groupId)"
  let row = mkRow "f4" "gF4" "active" 4 Nothing 100
  found <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    findWithKVConnector dbConf meshCfg [Is groupId (Eq ("gF4" :: Text))]
  check "groupId=gF4 ⇒ Just f4" $ found =?= (Just row)

tFindBySecondaryStatus :: IO Bool
tFindBySecondaryStatus = do
  putStrLn "\n[5] findWithKVConnector by the SECOND secondary index (status)"
  let row = mkRow "f5" "gF5" "uniqstat5" 5 Nothing 100
  found <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    findWithKVConnector dbConf meshCfg [Is status (Eq ("uniqstat5" :: Text))]
  check "status=uniqstat5 ⇒ Just f5" $ found =?= (Just row)

tFindMiss :: IO Bool
tFindMiss = do
  putStrLn "\n[6] findWithKVConnector — no such row ⇒ Right Nothing"
  found <- runFlow $ findWithKVConnector dbConf meshCfg [Is id (Eq ("nope-6" :: Text))]
  check "absent id ⇒ Right Nothing" $ found =?= Nothing

tFindDbFallback :: IO Bool
tFindDbFallback = do
  putStrLn "\n[7] findWithKVConnector — row only in Postgres (not KV) ⇒ DB fallback returns it"
  let row = mkRow "f7" "gF7" "active" 7 (Just "dbonly") 100
  found <- runFlow $ do
    L.runIO $ insertDbRow row
    findWithKVConnector dbConf meshCfg [Is id (Eq ("f7" :: Text))]
  check "DB-only row returned via fallback" $ found =?= (Just row)

tFindOneFromKvRedis :: IO Bool
tFindOneFromKvRedis = do
  putStrLn "\n[8] findOneFromKvRedis — Redis-only (no DB fallback)"
  let kvRow = mkRow "f8a" "gF8" "active" 8 Nothing 100
      dbRow = mkRow "f8b" "gF8b" "active" 8 Nothing 100
  (inRedis, dbOnly) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg kvRow
    L.runIO $ insertDbRow dbRow
    a <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("f8a" :: Text))]
    b <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("f8b" :: Text))]
    pure (a, b)
  ok1 <- check "in-Redis row ⇒ Just" $ inRedis =?= (Just kvRow)
  ok2 <- check "DB-only row ⇒ Nothing (no fallback)" $ dbOnly =?= Nothing
  pure (ok1 && ok2)

-- =============================================================================
-- READ — find all
-- =============================================================================

tFindAll :: IO Bool
tFindAll = do
  putStrLn "\n[9] findAllWithKVConnector by secondary ⇒ all matching rows"
  let rows = [mkRow "a1" "gA9" "active" 1 Nothing 100, mkRow "a2" "gA9" "active" 2 Nothing 200, mkRow "a3" "gA9" "active" 3 Nothing 300]
  res <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gA9" :: Text))]
  check "groupId=gA9 ⇒ {a1,a2,a3}" $ sortedIds res =?= ["a1", "a2", "a3"]

tFindAllEmpty :: IO Bool
tFindAllEmpty = do
  putStrLn "\n[10] findAllWithKVConnector — no matches ⇒ Right []"
  res <- runFlow $ findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gEMPTY" :: Text))]
  check "no matches ⇒ []" $ sortedIds res =?= []

tFindAllUnionDedup :: IO Bool
tFindAllUnionDedup = do
  putStrLn "\n[11] findAllWithKVConnector — KV ∪ DB, deduped by primary key"
  let kvRow = mkRow "u1" "gU11" "active" 1 Nothing 100
      dbRow = mkRow "u2" "gU11" "active" 2 Nothing 200
  res <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg kvRow -- in KV (and would drain to DB)
    L.runIO $ insertDbRow dbRow -- only in DB
    L.runIO $ insertDbRow kvRow -- also in DB (simulate drained) → must dedup to one
    findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gU11" :: Text))]
  check "KV row + DB-only row, KV row not duplicated ⇒ {u1,u2}" $ sortedIds res =?= ["u1", "u2"]

tFindAllFromKvRedis :: IO Bool
tFindAllFromKvRedis = do
  putStrLn "\n[12] findAllFromKvRedis — Redis only, no DB fallback"
  let kvRow = mkRow "k1" "gK12" "active" 1 Nothing 100
      dbRow = mkRow "k2" "gK12" "active" 2 Nothing 200
  res <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg kvRow
    L.runIO $ insertDbRow dbRow
    findAllFromKvRedis dbConf meshCfg [Is groupId (Eq ("gK12" :: Text))] Nothing
  check "only the KV row, DB row ignored ⇒ {k1}" $ sortedIds res =?= ["k1"]

tFindAllConditionalDb :: IO Bool
tFindAllConditionalDb = do
  putStrLn "\n[13] findAllWithKVAndConditionalDBInternal — KV hit serves KV; KV miss falls to DB"
  let kvRow = mkRow "cd1" "gCD13" "active" 1 Nothing 100
      dbRow = mkRow "cd2" "gCDDB" "active" 2 Nothing 200
  (kvHit, dbFall) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg kvRow
    L.runIO $ insertDbRow dbRow
    a <- findAllWithKVAndConditionalDBInternal dbConf meshCfg [Is groupId (Eq ("gCD13" :: Text))] Nothing
    b <- findAllWithKVAndConditionalDBInternal dbConf meshCfg [Is groupId (Eq ("gCDDB" :: Text))] Nothing
    pure (a, b)
  ok1 <- check "KV-present group ⇒ {cd1}" $ sortedIds kvHit =?= ["cd1"]
  ok2 <- check "KV-absent group ⇒ DB fallback {cd2}" $ sortedIds dbFall =?= ["cd2"]
  pure (ok1 && ok2)

tFindAllWithOptions :: IO Bool
tFindAllWithOptions = do
  putStrLn "\n[14] findAllWithOptionsKVConnector — order by updatedAt desc, limit/offset"
  let rows = [mkRow "o1" "gO14" "active" 1 Nothing 100, mkRow "o2" "gO14" "active" 2 Nothing 200, mkRow "o3" "gO14" "active" 3 Nothing 300]
  (top2, off1) <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    a <- findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("gO14" :: Text))] (Desc updatedAt) (Just 2) Nothing
    b <- findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("gO14" :: Text))] (Desc updatedAt) (Just 1) (Just 1)
    pure (a, b)
  ok1 <- check "desc updatedAt limit 2 ⇒ [o3,o2]" $ (ids <$> top2) =?= ["o3", "o2"]
  ok2 <- check "desc updatedAt limit 1 offset 1 ⇒ [o2]" $ (ids <$> off1) =?= ["o2"]
  pure (ok1 && ok2)

tFindAllWithOptionsNoOrder :: IO Bool
tFindAllWithOptionsNoOrder = do
  putStrLn "\n[15] findAllWithOptionsKVConnector' — limit/offset, no explicit order"
  let rows = [mkRow "n1" "gN15" "active" 1 Nothing 100, mkRow "n2" "gN15" "active" 2 Nothing 200, mkRow "n3" "gN15" "active" 3 Nothing 300]
  res <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    findAllWithOptionsKVConnector' dbConf meshCfg [Is groupId (Eq ("gN15" :: Text))] (Just 2) Nothing
  check "limit 2 ⇒ exactly 2 rows from the group" $ case res of
    Right rs -> length rs == 2 && all ((== "gN15") . groupId) rs
    Left _ -> False

tResidualFilter :: IO Bool
tResidualFilter = do
  putStrLn "\n[16] residual predicate on a non-indexed numeric column (cnt) filters in memory"
  let rows = [mkRow "r1" "gR16" "active" 7 Nothing 100, mkRow "r2" "gR16" "active" 9 Nothing 200]
  res <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gR16" :: Text)), Is cnt (Eq (7 :: Int))]
  check "groupId=gR16 AND cnt=7 ⇒ {r1}" $ sortedIds res =?= ["r1"]

-- =============================================================================
-- HALF-DRAINED DATASET — find* over a table where some matching rows have been
-- drained to Postgres (DB-only) and some have NOT (KV/Redis-only). Models the
-- real steady state: createWithKVConnector writes Redis + a drain-stream entry;
-- the (separate) drainer later lands it in PG. So at any instant a query's
-- result set spans tiers, and the connector must union them (and dedup a row
-- that is in BOTH because it was drained while still cached). insertDbRow puts a
-- row in PG only (drained + evicted); createWithKVConnector puts it in KV only
-- (not yet drained); doing both = drained but still cached.
-- =============================================================================

-- [35] findWithKVConnector (findOne): every row resolves regardless of tier —
-- KV-only via a Redis hit, DB-only via the DB fallback — and a findOne by a
-- secondary index over the mixed group returns one valid member.
tFindOneHalfDrained :: IO Bool
tFindOneHalfDrained = do
  putStrLn "\n[35] findOne — half-drained group: KV-only rows hit Redis, DB-only (drained) rows hit the DB fallback"
  let kv1 = mkRow "fo_kv1" "gFO35" "active" 1 (Just "kv") 100
      kv2 = mkRow "fo_kv2" "gFO35" "active" 2 (Just "kv") 200
      db1 = mkRow "fo_db1" "gFO35" "active" 3 (Just "db") 300
      db2 = mkRow "fo_db2" "gFO35" "active" 4 (Just "db") 400
  (fkv1, fkv2, fdb1, fdb2, bySecondary) <- runFlow $ do
    forM_ [kv1, kv2] (createWithKVConnector dbConf meshCfg) -- KV only (not drained)
    forM_ [db1, db2] (L.runIO . insertDbRow) -- DB only (drained, evicted from KV)
    a <- findWithKVConnector dbConf meshCfg [Is id (Eq ("fo_kv1" :: Text))]
    b <- findWithKVConnector dbConf meshCfg [Is id (Eq ("fo_kv2" :: Text))]
    c <- findWithKVConnector dbConf meshCfg [Is id (Eq ("fo_db1" :: Text))]
    d <- findWithKVConnector dbConf meshCfg [Is id (Eq ("fo_db2" :: Text))]
    e <- findWithKVConnector dbConf meshCfg [Is groupId (Eq ("gFO35" :: Text))]
    pure (a, b, c, d, e)
  ok1 <- check "KV-only fo_kv1 ⇒ Just (Redis hit)" $ fkv1 =?= Just kv1
  ok2 <- check "KV-only fo_kv2 ⇒ Just (Redis hit)" $ fkv2 =?= Just kv2
  ok3 <- check "DB-only fo_db1 ⇒ Just (DB fallback)" $ fdb1 =?= Just db1
  ok4 <- check "DB-only fo_db2 ⇒ Just (DB fallback)" $ fdb2 =?= Just db2
  ok5 <- check "findOne by secondary over mixed group ⇒ one valid member" $ case bySecondary of
    Right (Just r) -> id r `elem` (["fo_kv1", "fo_kv2", "fo_db1", "fo_db2"] :: [Text])
    _ -> False
  pure (ok1 && ok2 && ok3 && ok4 && ok5)

-- [36] findAllWithKVConnector: the full result set = KV-only ∪ DB-only, and a
-- row that is in BOTH tiers (drained but still cached) appears exactly once.
tFindAllHalfDrained :: IO Bool
tFindAllHalfDrained = do
  putStrLn "\n[36] findAll — half-drained group ⇒ KV ∪ DB union; a drained-but-still-cached row is deduped to one"
  let kvOnly = [mkRow "fa_kv1" "gFA36" "active" 1 Nothing 100, mkRow "fa_kv2" "gFA36" "active" 2 Nothing 200, mkRow "fa_kv3" "gFA36" "active" 3 Nothing 300]
      dbOnly = [mkRow "fa_db1" "gFA36" "active" 4 Nothing 400, mkRow "fa_db2" "gFA36" "active" 5 Nothing 500, mkRow "fa_db3" "gFA36" "active" 6 Nothing 600]
      both = mkRow "fa_both" "gFA36" "active" 7 Nothing 700
  res <- runFlow $ do
    forM_ kvOnly (createWithKVConnector dbConf meshCfg) -- not drained (KV only)
    forM_ dbOnly (L.runIO . insertDbRow) -- drained + evicted (DB only)
    _ <- createWithKVConnector dbConf meshCfg both -- drained but still cached: in KV
    L.runIO $ insertDbRow both -- ...and in DB
    findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gFA36" :: Text))]
  ok1 <-
    check "KV ∪ DB across tiers ⇒ all 7 ids" $
      sortedIds res =?= ["fa_both", "fa_db1", "fa_db2", "fa_db3", "fa_kv1", "fa_kv2", "fa_kv3"]
  ok2 <- check "the both-tiers row is not duplicated (length == 7)" $ case res of
    Right rs -> length rs == 7
    Left _ -> False
  pure (ok1 && ok2)

-- [37] findAll with IN queries over a half-drained dataset: an IN on the primary
-- key and an IN on a secondary key both return every existing row across tiers,
-- and silently ignore values that match nothing.
tFindAllInHalfDrained :: IO Bool
tFindAllInHalfDrained = do
  putStrLn "\n[37] findAll with IN (Is _ (In [...])) over half-drained data ⇒ all existing rows across tiers; absent values ignored"
  let kvOnly = [mkRow "in_kv1" "gIN37" "active" 1 Nothing 100, mkRow "in_kv2" "gIN37" "active" 2 Nothing 200]
      dbOnly = [mkRow "in_db1" "gIN37" "active" 3 Nothing 300, mkRow "in_db2" "gIN37" "active" 4 Nothing 400]
      queriedIds = ["in_kv1", "in_kv2", "in_db1", "in_db2", "in_absent"] :: [Text]
  (byIdIn, byGroupIn) <- runFlow $ do
    forM_ kvOnly (createWithKVConnector dbConf meshCfg) -- not drained (KV only)
    forM_ dbOnly (L.runIO . insertDbRow) -- drained + evicted (DB only)
    a <- findAllWithKVConnector dbConf meshCfg [Is id (In queriedIds)]
    b <- findAllWithKVConnector dbConf meshCfg [Is groupId (In (["gIN37", "gNONE37"] :: [Text]))]
    pure (a, b)
  ok1 <-
    check "IN over PKs spanning KV-only + DB-only ⇒ the 4 existing rows (absent id dropped)" $
      sortedIds byIdIn =?= ["in_db1", "in_db2", "in_kv1", "in_kv2"]
  ok2 <-
    check "IN over secondary groupId (one populated, one empty) ⇒ same 4 rows across tiers" $
      sortedIds byGroupIn =?= ["in_db1", "in_db2", "in_kv1", "in_kv2"]
  pure (ok1 && ok2)

-- =============================================================================
-- UPDATE
-- =============================================================================

tUpdateReturning :: IO Bool
tUpdateReturning = do
  putStrLn "\n[17] updateWithKVConnector — returns updated row; Redis value reflects it"
  let row = mkRow "up1" "gUP17" "active" 1 Nothing 100
  (res, reread) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    r <- updateWithKVConnector dbConf dbConf meshCfg [Set payload ("changed" :: Text)] [Is id (Eq ("up1" :: Text))]
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("up1" :: Text))]
    pure (r, f)
  ok1 <- check "returns Just row with payload=changed" $ (fmap payload <$> res) =?= (Just "changed")
  ok2 <- check "subsequent read sees payload=changed" $ (fmap payload <$> reread) =?= (Just "changed")
  pure (ok1 && ok2)

tUpdateWoReturning :: IO Bool
tUpdateWoReturning = do
  putStrLn "\n[18] updateWoReturningWithKVConnector — returns (); change is persisted"
  let row = mkRow "up2" "gUP18" "active" 1 Nothing 100
  (res, reread) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    r <- updateWoReturningWithKVConnector dbConf dbConf meshCfg [Set cnt (42 :: Int)] [Is id (Eq ("up2" :: Text))]
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("up2" :: Text))]
    pure (r, f)
  ok1 <- check "returns Right ()" $ res =?= ()
  ok2 <- check "cnt is now 42" $ (fmap cnt <$> reread) =?= (Just 42)
  pure (ok1 && ok2)

tUpdateMovesSecondaryIndex :: IO Bool
tUpdateMovesSecondaryIndex = do
  putStrLn "\n[19] updateWithKVConnector changing groupId moves the pkey between secondary SETs"
  let row = mkRow "mv1" "gOLD" "active" 1 Nothing 100
  (oldHas, newHas, foundNew) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- updateWithKVConnector dbConf dbConf meshCfg [Set groupId ("gNEW" :: Text)] [Is id (Eq ("mv1" :: Text))]
    a <- sIsMember connName (groupSetKey "gOLD") (pkOf row)
    b <- sIsMember connName (groupSetKey "gNEW") (pkOf row)
    f <- findWithKVConnector dbConf meshCfg [Is groupId (Eq ("gNEW" :: Text))]
    pure (a, b, f)
  ok1 <- check "old SET (gOLD) no longer has the pkey" (not oldHas)
  ok2 <- check "new SET (gNEW) has the pkey" newHas
  ok3 <- check "findable under new groupId" $ (fmap id <$> foundNew) =?= (Just "mv1")
  pure (ok1 && ok2 && ok3)

tUpdateAdditiveMerge :: IO Bool
tUpdateAdditiveMerge = do
  putStrLn "\n[20] additive merge — setting one column preserves the others (incl. a Just note)"
  let row = mkRow "am1" "gAM20" "active" 1 (Just "keepme") 100
  reread <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- updateWithKVConnector dbConf dbConf meshCfg [Set payload ("newpay" :: Text)] [Is id (Eq ("am1" :: Text))]
    findWithKVConnector dbConf meshCfg [Is id (Eq ("am1" :: Text))]
  ok1 <- check "payload updated" $ (fmap payload <$> reread) =?= (Just "newpay")
  ok2 <- check "note preserved (= Just keepme)" $ (fmap note <$> reread) =?= (Just (Just "keepme"))
  ok3 <- check "cnt preserved (= 1)" $ (fmap cnt <$> reread) =?= (Just 1)
  pure (ok1 && ok2 && ok3)

tUpdateAllReturning :: IO Bool
tUpdateAllReturning = do
  putStrLn "\n[21] updateAllReturningWithKVConnector — bulk update, returns all updated rows"
  let rows = [mkRow "ua1" "gUA21" "active" 1 Nothing 100, mkRow "ua2" "gUA21" "active" 2 Nothing 200]
  (res, rereads) <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    r <- updateAllReturningWithKVConnector dbConf dbConf meshCfg [Set status ("done" :: Text)] [Is groupId (Eq ("gUA21" :: Text))]
    f <- findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gUA21" :: Text))]
    pure (r, f)
  ok1 <- check "returns both updated rows {ua1,ua2}" $ sortedIds res =?= ["ua1", "ua2"]
  ok2 <- check "all rows now status=done" $ case rereads of
    Right rs -> not (null rs) && all ((== "done") . status) rs
    Left _ -> False
  pure (ok1 && ok2)

tUpdateAll :: IO Bool
tUpdateAll = do
  putStrLn "\n[22] updateAllWithKVConnector — bulk update without returning"
  let rows = [mkRow "al1" "gAL22" "active" 1 Nothing 100, mkRow "al2" "gAL22" "active" 2 Nothing 200]
  (res, rereads) <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    r <- updateAllWithKVConnector dbConf dbConf meshCfg [Set cnt (5 :: Int)] [Is groupId (Eq ("gAL22" :: Text))]
    f <- findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gAL22" :: Text))]
    pure (r, f)
  ok1 <- check "returns Right ()" $ res =?= ()
  ok2 <- check "all rows now cnt=5" $ case rereads of
    Right rs -> length rs == 2 && all ((== 5) . cnt) rs
    Left _ -> False
  pure (ok1 && ok2)

-- =============================================================================
-- DELETE
-- =============================================================================

tDeleteTombstone :: IO Bool
tDeleteTombstone = do
  putStrLn "\n[23] deleteWithKVConnector — writes a tombstone; row reads back as gone"
  let row = mkRow "del1" "gDEL23" "active" 1 Nothing 100
  (res, found, fromRedis) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    r <- deleteWithKVConnector dbConf meshCfg [Is id (Eq ("del1" :: Text))]
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("del1" :: Text))]
    g <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("del1" :: Text))]
    pure (r, f, g)
  ok1 <- check "returns Right ()" $ res =?= ()
  ok2 <- check "findWithKVConnector ⇒ Nothing (dead)" $ found =?= Nothing
  ok3 <- check "findOneFromKvRedis ⇒ Nothing (tombstone)" $ fromRedis =?= Nothing
  pure (ok1 && ok2 && ok3)

tDeleteReturning :: IO Bool
tDeleteReturning = do
  putStrLn "\n[24] deleteReturningWithKVConnector — returns the deleted row"
  let row = mkRow "del2" "gDEL24" "active" 1 Nothing 100
  (res, found) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    r <- deleteReturningWithKVConnector dbConf meshCfg [Is id (Eq ("del2" :: Text))]
    f <- findWithKVConnector dbConf meshCfg [Is id (Eq ("del2" :: Text))]
    pure (r, f)
  ok1 <- check "returns Just (deleted row del2)" $ res =?= (Just row)
  ok2 <- check "subsequent find ⇒ Nothing" $ found =?= Nothing
  pure (ok1 && ok2)

tDeleteAllReturning :: IO Bool
tDeleteAllReturning = do
  putStrLn "\n[25] deleteAllReturningWithKVConnector — bulk delete, returns all deleted rows"
  let rows = [mkRow "da1" "gDA25" "active" 1 Nothing 100, mkRow "da2" "gDA25" "active" 2 Nothing 200]
  (res, remaining) <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    r <- deleteAllReturningWithKVConnector dbConf meshCfg [Is groupId (Eq ("gDA25" :: Text))]
    f <- findAllWithKVConnector dbConf meshCfg [Is groupId (Eq ("gDA25" :: Text))]
    pure (r, f)
  ok1 <- check "returns both deleted rows {da1,da2}" $ sortedIds res =?= ["da1", "da2"]
  ok2 <- check "group is now empty" $ sortedIds remaining =?= []
  pure (ok1 && ok2)

-- =============================================================================
-- MULTI-CLOUD
-- =============================================================================

tMultiCloudFindPrimaryWins :: IO Bool
tMultiCloudFindPrimaryWins = do
  putStrLn "\n[26] multi-cloud findWithKVConnector — a primary HIT wins, secondary not consulted"
  let primaryRow = mkRow "mc1" "gMC26" "active" 1 (Just "primary") 100
      secondaryRow = mkRow "mc1" "gMC26" "active" 1 (Just "secondary") 100
  found <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg primaryRow -- 6379
    _ <- createWithKVConnector dbConf cfgSecondaryAsPrimary secondaryRow -- 6380
    findWithKVConnector dbConf cfgMultiCloud [Is id (Eq ("mc1" :: Text))]
  check "primary copy (note=Just primary) wins" $ (fmap note <$> found) =?= (Just (Just "primary"))

tMultiCloudDedup :: IO Bool
tMultiCloudDedup = do
  putStrLn "\n[27] multi-cloud findAllWithOptions — a row in BOTH clouds appears once"
  let row = mkRow "mc2" "gMC27" "active" 1 Nothing 100
  res <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row -- 6379
    _ <- createWithKVConnector dbConf cfgSecondaryAsPrimary row -- 6380
    findAllWithOptionsKVConnector dbConf cfgMultiCloud [Is groupId (Eq ("gMC27" :: Text))] (Desc updatedAt) (Just 10) Nothing
  check "row present in both clouds ⇒ returned once" $ (ids <$> res) =?= ["mc2"]

-- =============================================================================
-- ERROR / EDGE
-- =============================================================================

tFindOneOfMany :: IO Bool
tFindOneOfMany = do
  putStrLn "\n[28] findWithKVConnector where a secondary index matches >1 row ⇒ returns ONE (listToMaybe), no error"
  let rows = [mkRow "me1" "gME28" "active" 1 Nothing 100, mkRow "me2" "gME28" "active" 2 Nothing 200]
  res <- runFlow $ do
    forM_ rows (createWithKVConnector dbConf meshCfg)
    findWithKVConnector dbConf meshCfg [Is groupId (Eq ("gME28" :: Text))]
  check "two matches on a single-row find ⇒ Right (Just one-of-them), not an error" $ case res of
    Right (Just r) -> id r `elem` (["me1", "me2"] :: [Text])
    _ -> False

-- =============================================================================
-- ADDED STRICT TESTS — close the gaps a strictness audit found:
--   * the cross-cloud read path was never actually exercised (rows were always
--     in the primary too, so the primary-hit short-circuit masked it);
--   * delete only checked find⇒Nothing, indistinguishable from a hard DEL;
--   * setting a Maybe column back to Nothing was untested.
-- =============================================================================

-- [29] The REAL multi-cloud invariant: a row that lives ONLY in the secondary
-- cloud (6380) must be returned by a multi-cloud read (primary MISS → secondary).
-- A primary-only read must NOT see it — proving it is genuinely secondary-only,
-- so this would fail if the secondary-read branch were broken.
tMultiCloudSecondaryOnlyRead :: IO Bool
tMultiCloudSecondaryOnlyRead = do
  putStrLn "\n[29] multi-cloud: a SECONDARY-only row (6380) is read via primary-miss→secondary"
  let row = mkRow "scs1" "gSCS" "active" 1 (Just "only-on-secondary") 100
  (viaMultiByPk, viaMultiBySecKey, primaryOnly, viaMultiAll) <- runFlow $ do
    _ <- createWithKVConnector dbConf cfgSecondaryAsPrimary row -- write to 6380 ONLY
    a <- findWithKVConnector dbConf cfgMultiCloud [Is id (Eq ("scs1" :: Text))]
    b <- findWithKVConnector dbConf cfgMultiCloud [Is groupId (Eq ("gSCS" :: Text))]
    c <- findWithKVConnector dbConf meshCfg [Is id (Eq ("scs1" :: Text))] -- primary-only cfg
    d <- findAllWithKVConnector dbConf cfgMultiCloud [Is groupId (Eq ("gSCS" :: Text))]
    pure (a, b, c, d)
  ok1 <- check "multi-cloud find by pk ⇒ Just (read from secondary)" $ viaMultiByPk =?= Just row
  ok2 <- check "multi-cloud find by secondary index ⇒ Just" $ viaMultiBySecKey =?= Just row
  ok3 <- check "primary-only find ⇒ Nothing (row is genuinely NOT on primary)" $ primaryOnly =?= Nothing
  ok4 <- check "multi-cloud findAll ⇒ {scs1}" $ sortedIds viaMultiAll =?= ["scs1"]
  pure (ok1 && ok2 && ok3 && ok4)

-- [30] delete writes a TOMBSTONE, not a hard DEL: the pkey value still exists in
-- Redis and decodes as dead ("DEAD" prefix), and the secondary SET still contains
-- the pkey. Distinguishes the real behaviour from a DEL (which would leave nil).
tDeleteTombstoneMarker :: IO Bool
tDeleteTombstoneMarker = do
  putStrLn "\n[30] deleteWithKVConnector writes a DEAD tombstone (not a hard DEL); SET membership kept"
  let row = mkRow "tomb1" "gTOMB" "active" 1 Nothing 100
  (rawAfter, setHasAfter) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- deleteWithKVConnector dbConf meshCfg [Is id (Eq ("tomb1" :: Text))]
    v <- redisGetRaw connName (pkOf row)
    m <- sIsMember connName (groupSetKey "gTOMB") (pkOf row)
    pure (v, m)
  ok1 <- check "pkey value still present after delete (tombstone, not nil)" $ isJust rawAfter
  ok2 <- check "stored value is DEAD-marked ('DEAD' prefix)" $ case rawAfter of
    Just bs -> "DEAD" `isPrefxOf` bs
    Nothing -> False
  ok3 <- check "secondary SET still contains the pkey (tombstone leaves index membership)" setHasAfter
  pure (ok1 && ok2 && ok3)

-- [31] Setting a Maybe column back to Nothing nulls it (the merge inserts
-- note:null). Covers the additive-merge Nothing path that was untested.
tUpdateMaybeToNothing :: IO Bool
tUpdateMaybeToNothing = do
  putStrLn "\n[31] update Set note Nothing ⇒ field becomes Nothing (other fields preserved)"
  let row = mkRow "mn1" "gMN" "active" 7 (Just "present") 100
  reread <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- updateWithKVConnector dbConf dbConf meshCfg [Set note (Nothing :: Maybe Text)] [Is id (Eq ("mn1" :: Text))]
    findWithKVConnector dbConf meshCfg [Is id (Eq ("mn1" :: Text))]
  ok1 <- check "note is now Nothing" $ (fmap note <$> reread) =?= Just Nothing
  ok2 <- check "cnt preserved (= 7)" $ (fmap cnt <$> reread) =?= Just 7
  ok3 <- check "status preserved (= active)" $ (fmap status <$> reread) =?= Just "active"
  pure (ok1 && ok2 && ok3)

-- bytes-prefix check without pulling in extra deps
isPrefxOf :: ByteString -> ByteString -> Bool
isPrefxOf p b = BS.take (BS.length p) b == p

-- [34] The JSONB fallback ("json fix"): when the Beam read hits Postgres error
-- 42703 (a column the record expects is missing from the table — schema drift),
-- the connector must recover by re-reading the row via a jsonb query instead of
-- erroring. We reproduce 42703 by dropping the nullable `note` column, then
-- restore it. Without the fallback, findAllWithKVConnector would return Left.
tJsonbFallbackOnSchemaDrift :: IO Bool
tJsonbFallbackOnSchemaDrift = do
  putStrLn "\n[34] JSONB fallback: a DB read against a drifted schema (missing column) recovers via jsonb"
  -- Postgres-only row, written before the column is dropped.
  runRaw "ALTER TABLE kv_live DROP COLUMN IF EXISTS note"
  runRaw "INSERT INTO kv_live (id,group_id,status,cnt,updated_at,payload) VALUES ('jb1','gJB','active',1,'2026-01-01T00:00:00Z','p-jb1')"
  res <- runFlow $ findAllWithKVConnector dbConf meshCfg [Is id (Eq ("jb1" :: Text))]
  runRaw "ALTER TABLE kv_live ADD COLUMN IF NOT EXISTS note text" -- restore for the other tests
  ok1 <- check "find recovers the row (jsonb fallback fired, not an error)" $ sortedIds res =?= ["jb1"]
  ok2 <- check "missing nullable column decodes to Nothing" $ case res of
    Right [r] -> note r == Nothing && payload r == "p-jb1"
    _ -> False
  pure (ok1 && ok2)

-- [32] kvHardKilled is the kill-switch: reads must bypass Redis and hit Postgres.
-- A Redis-only row becomes invisible; a Postgres-only row becomes visible.
tHardKilled :: IO Bool
tHardKilled = do
  putStrLn "\n[32] kvHardKilled ⇒ reads bypass Redis and hit Postgres directly"
  let cfgKill = meshCfg {kvHardKilled = True}
      kvRow = mkRow "hk1" "gHK" "active" 1 Nothing 100 -- Redis only
      dbRow = mkRow "hk2" "gHK" "active" 2 Nothing 200 -- Postgres only
  (kvOnly, dbOnly) <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg kvRow
    L.runIO $ insertDbRow dbRow
    a <- findWithKVConnector dbConf cfgKill [Is id (Eq ("hk1" :: Text))]
    b <- findWithKVConnector dbConf cfgKill [Is id (Eq ("hk2" :: Text))]
    pure (a, b)
  ok1 <- check "Redis-only row is NOT found under kvHardKilled (Redis bypassed)" $ kvOnly =?= Nothing
  ok2 <- check "Postgres-only row IS found under kvHardKilled (served from DB)" $ dbOnly =?= Just dbRow
  pure (ok1 && ok2)

-- [33] Exercise the TH-generated MeshMeta clause decoders directly (the library's
-- read path resolves clauses via Beam descriptors, so these would otherwise be
-- untested). Valid columns decode; unknown columns fail.
tParseClauses :: IO Bool
tParseClauses = do
  putStrLn "\n[33] TH-generated parseFieldAndGetClause/parseSetClause: decode valid columns, reject unknown"
  let runP :: Parser a -> Maybe a
      runP p = parseMaybe (const p) ()
      validWhere = isJust (runP (parseFieldAndGetClause (A.String "g") "groupId" :: Parser (TermWrap BP.Postgres KvLiveT)))
      unknownWhere = isNothing (runP (parseFieldAndGetClause (A.String "g") "nosuchcol" :: Parser (TermWrap BP.Postgres KvLiveT)))
      validSet = isJust (runP (parseSetClause [("payload", A.String "x")] :: Parser [Set BP.Postgres KvLiveT]))
      unknownSet = isNothing (runP (parseSetClause [("nope", A.String "x")] :: Parser [Set BP.Postgres KvLiveT]))
  ok1 <- check "Where clause: valid column 'groupId' ⇒ parses" validWhere
  ok2 <- check "Where clause: unknown column ⇒ fails" unknownWhere
  ok3 <- check "Set clause: valid column 'payload' ⇒ parses" validSet
  ok4 <- check "Set clause: unknown column ⇒ fails" unknownSet
  pure (ok1 && ok2 && ok3 && ok4)

-- =============================================================================
-- Recache flags — run in a CHILD process that sets IS_CACHING_DB_FIND_ENABLED
-- and IS_RECACHING_ENABLED=True (these are read once per process as CAFs, so
-- they can't be toggled in the main run). See the orchestrator's --recache-phase.
-- =============================================================================
runRecacheTests :: IO Bool
runRecacheTests = do
  putStrLn "\n############################################################"
  putStrLn "#  recache flags (IS_CACHING_DB_FIND_ENABLED / IS_RECACHING_ENABLED = True)"
  putStrLn "############################################################"
  ensureTable
  cleanPg
  runFlow flushTestKeys
  oks <- sequence [tCachingDbFindRecache, tUpdateRecache]
  let passed = length (filter (== True) oks)
  putStrLn $ "\n=== recache: " <> show passed <> "/" <> show (length oks) <> " passed ==="
  pure (and oks)

-- IS_CACHING_DB_FIND_ENABLED: a find that misses Redis and reads Postgres
-- recaches the row, so a follow-up Redis-only read now finds it.
tCachingDbFindRecache :: IO Bool
tCachingDbFindRecache = do
  putStrLn "\n[R1] IS_CACHING_DB_FIND_ENABLED: find miss→DB recaches the row into Redis"
  let row = mkRow "rc1" "gRC" "active" 1 Nothing 100
  (found, inRedisBefore, inRedisAfter) <- runFlow $ do
    L.runIO $ insertDbRow row -- Postgres only
    before <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("rc1" :: Text))]
    a <- findWithKVConnector dbConf meshCfg [Is id (Eq ("rc1" :: Text))] -- miss→DB→recache
    after <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("rc1" :: Text))]
    pure (a, before, after)
  ok0 <- check "precondition: row absent from Redis before the find" $ inRedisBefore =?= Nothing
  ok1 <- check "find returns the DB row" $ found =?= Just row
  ok2 <- check "row is NOW in Redis (recached by the find)" $ inRedisAfter =?= Just row
  pure (ok0 && ok1 && ok2)

-- IS_RECACHING_ENABLED: an update on a Redis-missing row pulls it from DB,
-- recaches it pod-local, and applies the update.
tUpdateRecache :: IO Bool
tUpdateRecache = do
  putStrLn "\n[R2] IS_RECACHING_ENABLED: update on a Redis-missing row recaches it + applies the change"
  let row = mkRow "rc2" "gRC2" "active" 5 Nothing 100
  (upd, inRedisAfter) <- runFlow $ do
    L.runIO $ insertDbRow row -- Postgres only
    a <- updateWithKVConnector dbConf dbConf meshCfg [Set cnt (99 :: Int)] [Is id (Eq ("rc2" :: Text))]
    b <- findOneFromKvRedis dbConf meshCfg [Is id (Eq ("rc2" :: Text))]
    pure (a, b)
  ok1 <- check "update returns the row with cnt=99" $ (fmap cnt <$> upd) =?= Just 99
  ok2 <- check "row is NOW in Redis with cnt=99 (recached pod-local)" $ (fmap cnt <$> inRedisAfter) =?= Just 99
  pure (ok1 && ok2)
