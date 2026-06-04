{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans -Wno-missing-methods -Wno-name-shadowing -Wno-unused-imports #-}

-- =============================================================================
-- Live, end-to-end tests for the sorted-secondary-index (ZSET) feature.
--
-- Exercises the REAL connector code (createWithKVConnector, findAllWithOptionsKVConnector,
-- updateWithKVConnector, deleteWithKVConnector) against a real Postgres + Redis, plus the
-- raw ZSET primitives and the multi-cloud union path.
--
-- Postgres only (matches production). Phase is read from KV_SORTED_INDEX_MODE and is fixed
-- per process, so run once per phase:
--
--   <local postgres on 5432, db euler_sorted_test, table kv_sorted>
--   redis-server --port 6379 ; redis-server --port 6380
--   cabal run euler-hs:exe:sorted-set-live-test
--   KV_SORTED_INDEX_MODE=DUAL      cabal run euler-hs:exe:sorted-set-live-test
--   KV_SORTED_INDEX_MODE=ZSET_ONLY cabal run euler-hs:exe:sorted-set-live-test
-- =============================================================================

module SortedSetTests (runSortedSetTests) where

import qualified Data.Aeson as A
import qualified Data.HashMap.Strict as HM
import Data.List (sort)
import qualified Data.Serialize as Cereal
import qualified Data.Text as T
import Data.Time (UTCTime (..), addUTCTime, fromGregorian, secondsToDiffTime)
import qualified Database.Beam as B
import qualified Database.Beam.Postgres as BP
import qualified Database.PostgreSQL.Simple as PGS
import qualified EulerHS.Interpreters as I
import EulerHS.KVConnector.Flow
  ( createWithKVConnector,
    deleteWithKVConnector,
    findAllWithOptionsKVConnector,
    updateWithKVConnector,
  )
import EulerHS.KVConnector.Types
import EulerHS.KVConnector.Utils
  ( getPKeyWithShard,
    getPrimaryKeyFromFieldsAndValues,
    getSecondaryLookupKeys,
    defaultSortedIndexMode,
    shouldReadZSet,
    shouldWriteSet,
    shouldWriteZSet,
    toZSetKey,
  )
import qualified EulerHS.Language as L
import EulerHS.Prelude hiding (id, putStrLn, show)
import qualified EulerHS.Runtime as R
import EulerHS.Types
  ( DBConfig,
    PoolConfig (..),
    PostgresConfig (..),
    mkPostgresPoolConfig,
  )
import qualified EulerHS.Types as T
import Sequelize (Clause (..), ModelMeta (..), OrderBy (..), Set (..), Term (..), Where)
import Prelude (Show (show), String, putStrLn)

-- =============================================================================
-- Table: kv_sorted (id pk, group_id secondary index, updated_at = ZSET score)
-- =============================================================================

data KVSortedT f = KVSortedT
  { id :: B.C f Text,
    groupId :: B.C f Text,
    updatedAt :: B.C f UTCTime,
    payload :: B.C f Text
  }
  deriving (Generic, B.Beamable)

instance B.Table KVSortedT where
  data PrimaryKey KVSortedT f = KVSortedId (B.C f Text) deriving (Generic, B.Beamable)
  primaryKey = KVSortedId . id

type KVSorted = KVSortedT Identity

deriving instance Show KVSorted

deriving instance Eq KVSorted

instance A.ToJSON KVSorted where toJSON = A.genericToJSON A.defaultOptions

instance A.FromJSON KVSorted where parseJSON = A.genericParseJSON A.defaultOptions

instance Cereal.Serialize KVSorted where
  put = error "cereal disabled"
  get = error "cereal disabled"

-- SQL column names (snake_case) — used by ModelMeta for the actual Postgres query.
kvSortedModSql :: KVSortedT (B.FieldModification (B.TableField KVSortedT))
kvSortedModSql =
  B.tableModification
    { id = B.fieldNamed "id",
      groupId = B.fieldNamed "group_id",
      updatedAt = B.fieldNamed "updated_at",
      payload = B.fieldNamed "payload"
    }

-- Haskell field names (camelCase) — used by MeshMeta for the KV/redis side, so it stays
-- consistent with KVConnector.secondaryKeys and the row's genericToJSON keys. This mirrors
-- how the codegen macro wires ModMesh (nameBase) vs the SQL Mod (quietSnake).
kvSortedModMesh :: KVSortedT (B.FieldModification (B.TableField KVSortedT))
kvSortedModMesh =
  B.tableModification
    { id = B.fieldNamed "id",
      groupId = B.fieldNamed "groupId",
      updatedAt = B.fieldNamed "updatedAt",
      payload = B.fieldNamed "payload"
    }

instance ModelMeta KVSortedT where
  modelFieldModification = kvSortedModSql
  modelTableName = "kv_sorted"
  modelSchemaName = Nothing

instance MeshMeta BP.Postgres KVSortedT where
  meshModelFieldModification = kvSortedModMesh
  valueMapper = mempty
  parseFieldAndGetClause _ _ = fail "parseFieldAndGetClause: not exercised by this test"
  parseSetClause _ = fail "parseSetClause: not exercised by this test"

instance KVConnector (KVSortedT Identity) where
  tableName = "kvSorted"
  keyMap = HM.fromList [("id", True), ("groupId", False)]
  primaryKey r = PKey [("id", id r)]
  secondaryKeys r = [SKey [("groupId", groupId r)]]
  mkSQLObject = A.toJSON
  sortScore = Just (\r -> toZScore (updatedAt r))
  sortScoreColumn = Just "updatedAt"

instance TableMappings (KVSortedT Identity) where
  getTableName = "kv_sorted"
  getTableMappings = []

-- =============================================================================
-- Config & helpers
-- =============================================================================

dbConf :: DBConfig BP.Pg
dbConf =
  mkPostgresPoolConfig
    "sorted-live"
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

-- The rollout phase under test. The connector reads it from MeshConfig.sortedIndexMode
-- (NOT a global env), so we simply put it on the config. We pick it from the
-- KV_SORTED_INDEX_MODE env here only to drive the 3 phases of this harness.
mode :: SortedIndexMode
mode = defaultSortedIndexMode

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
      sortedIndexMode = mode
    }

cfgPrimaryOnly, cfgSecondaryAsPrimary, cfgMultiCloud :: MeshConfig
cfgPrimaryOnly = meshCfg {kvRedis = connName, secondaryRedisEnabled = False}
cfgSecondaryAsPrimary = meshCfg {kvRedis = connNameSecondary, secondaryRedisEnabled = False}
cfgMultiCloud = meshCfg {kvRedis = connName, kvRedisSecondary = connNameSecondary, secondaryRedisEnabled = True}

baseTime :: UTCTime
baseTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 0)

mkRow :: Text -> Text -> Integer -> KVSorted
mkRow i g secs = KVSortedT {id = i, groupId = g, updatedAt = addUTCTime (fromIntegral secs) baseTime, payload = "p-" <> i}

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

cleanPg :: IO ()
cleanPg = runRaw "DELETE FROM kv_sorted"

-- Make a run hermetic: drop every key this test owns on BOTH clouds before starting.
-- Only the test's own keyspace is touched (kvSorted_*, the Z:: sorted-index mirror, and the
-- raw-primitive scratch key) — never a global FLUSH, so any other data on these Redis
-- instances is left intact. Without this, a member written by a previous phase (TTL 1h) would
-- linger and make a later phase observe an index entry it never wrote.
flushTestKeys :: L.Flow ()
flushTestKeys =
  forM_ [connName, connNameSecondary] $ \conn ->
    forM_ ["kvSorted*", "Z::kvSorted*", "live:zset:*"] $ \pat -> do
      eks <- L.runKVDB conn $ L.rawRequest ["KEYS", enc pat]
      case eks of
        Right (ks :: [ByteString]) -> unless (null ks) . void . L.runKVDB conn $ L.del ks
        Left _ -> pure ()

check :: String -> Bool -> IO Bool
check label ok = do
  putStrLn $ "  " <> (if ok then "[PASS] " else "[FAIL] ") <> label
  pure ok

-- secondary SET key / ZSET key for a groupId value
setKeyFor :: Text -> Text
setKeyFor g = case getSecondaryLookupKeys "" (mkRow "x" g 0) of (k : _) -> k; [] -> ""

zKeyFor :: Text -> Text
zKeyFor = toZSetKey . setKeyFor

-- exactly what the connector stores as the pkey/member (incl. shard hash-tag)
pkOf :: KVSorted -> ByteString
pkOf r = enc $ getPKeyWithShard r meshCfg.redisKeyPrefix meshCfg.tableShardModRange

-- =============================================================================
-- Tests
-- =============================================================================

-- | Run the sorted-secondary-index (ZSET) suite for the rollout phase given by
--   the KV_SORTED_INDEX_MODE env var (read once, per-process). Returns True iff
--   all assertions passed. Cleans its own PG rows + Redis keys.
runSortedSetTests :: IO Bool
runSortedSetTests = do
  putStrLn $ "\n############################################################"
  putStrLn $ "#  sorted-set-live  [phase=" <> show mode <> "]"
  putStrLn $ "############################################################"
  runFlow flushTestKeys -- hermetic: clear this test's keyspace on both clouds before starting
  -- INDEPENDENT oracle: the SET/ZSET we EXPECT per rollout phase, hardcoded from
  -- the rollout spec — NOT derived from shouldWriteSet/shouldWriteZSet (the
  -- functions under test). This way a sign-flip in those functions is caught
  -- instead of being masked by a self-referential expectation.
  let (expectSet, expectZ) = case mode of
        SetOnly -> (True, False) -- legacy: SET only
        DualWrite -> (True, True) -- warm-up: write both
        ZSetOnly -> (False, True) -- final: ZSET only

  -- ---- 1. raw ZSET primitives (mode independent) -------------------------
  putStrLn "\n[1] ZSET primitives (zadd/zrevrangebyscorewithlimit/zrangebyscorewithlimit/zrem/zrange/re-score)"
  prim <- runFlow $ do
    let k = enc "live:zset:prim"
    _ <- L.runKVDB connName $ L.del [k]
    _ <- L.runKVDB connName $ L.zadd k [(100, "a"), (200, "b"), (300, "c")]
    d2 <- L.runKVDB connName $ L.zrevrangebyscorewithlimit k (1 / 0) (negate (1 / 0)) 0 2
    a1 <- L.runKVDB connName $ L.zrangebyscorewithlimit k (negate (1 / 0)) (1 / 0) 1 1
    _ <- L.runKVDB connName $ L.zrem k ["b"]
    rem' <- L.runKVDB connName $ L.zrange k 0 (-1)
    _ <- L.runKVDB connName $ L.zadd k [(50, "a")]
    res' <- L.runKVDB connName $ L.zrange k 0 (-1)
    _ <- L.runKVDB connName $ L.del [k]
    pure (d2, a1, rem', res')
  ok1 <- check "primitives" (prim == (Right ["c", "b"], Right ["b"], Right ["a", "c"], Right ["a", "c"]))

  -- ---- 2. createWithKVConnector → membership per phase -------------------
  putStrLn "\n[2] createWithKVConnector maintains SET/ZSET per phase"
  cleanPg
  (sm, zm) <- runFlow $ do
    let row = mkRow "c1" "gC" 100
    _ <- createWithKVConnector dbConf meshCfg row
    s <- L.runKVDB connName $ L.smembers (enc (setKeyFor "gC"))
    z <- L.runKVDB connName $ L.zrange (enc (zKeyFor "gC")) 0 (-1)
    pure (s, z)
  ok2a <- check ("SET membership == " <> show expectSet) (sm == Right [pkOf (mkRow "c1" "gC" 100) | expectSet])
  ok2b <- check ("ZSET membership == " <> show expectZ) (zm == Right [pkOf (mkRow "c1" "gC" 100) | expectZ])

  -- ---- 3. findAllWithOptionsKVConnector ordered + limited (KV hit) -------
  putStrLn "\n[3] findAllWithOptions Desc updatedAt limit 2 (fast-path in ZSetOnly, in-mem sort otherwise)"
  cleanPg
  r3 <- runFlow $ do
    forM_ [mkRow "f1" "gF" 100, mkRow "f2" "gF" 200, mkRow "f3" "gF" 300] (createWithKVConnector dbConf meshCfg)
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("gF" :: Text))] (Desc updatedAt) (Just 2) Nothing
  ok3 <- check "top-2 by updatedAt desc == [f3, f2]" $ case r3 of
    Right rows -> map id rows == ["f3", "f2"]
    Left _ -> False

  -- ---- 4. DB fallback: rows only in Postgres come back via the merge -----
  putStrLn "\n[4] DB fallback — row only in Postgres (not in KV) is returned"
  cleanPg
  runRaw "INSERT INTO kv_sorted (id,group_id,updated_at,payload) VALUES ('db1','gDB','2026-02-01T00:00:00Z','only-in-pg')"
  r4 <- runFlow $ findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("gDB" :: Text))] (Desc updatedAt) (Just 10) Nothing
  ok4 <- check "DB-only row returned via fallback" $ case r4 of
    Right rows -> map id rows == ["db1"]
    Left _ -> False

  -- ---- 5. update: changing the secondary value moves the index entry -----
  putStrLn "\n[5] updateWithKVConnector moves pkey between secondary indexes"
  cleanPg
  (oldHas, newHas) <- runFlow $ do
    let row = mkRow "u1" "gOld" 100
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- updateWithKVConnector dbConf dbConf meshCfg [Set groupId ("gNew" :: Text)] [Is id (Eq ("u1" :: Text))]
    let readK g =
          if expectZ
            then L.runKVDB connName $ L.zrange (enc (zKeyFor g)) 0 (-1)
            else L.runKVDB connName $ L.smembers (enc (setKeyFor g))
    o <- readK "gOld"
    n <- readK "gNew"
    pure (o, n)
  let upk = pkOf (mkRow "u1" "gNew" 100)
  ok5a <- check "old index no longer has pkey" (oldHas == Right [])
  ok5b <- check "new index has pkey" (newHas == Right [upk])

  -- ---- 6. delete: ZREM / SREM removes the pkey from the index ------------
  putStrLn "\n[6] deleteWithKVConnector removes pkey from the index"
  cleanPg
  afterDel <- runFlow $ do
    let row = mkRow "d1" "gDel" 100
    _ <- createWithKVConnector dbConf meshCfg row
    _ <- deleteWithKVConnector dbConf meshCfg [Is id (Eq ("d1" :: Text))]
    if expectZ
      then L.runKVDB connName $ L.zrange (enc (zKeyFor "gDel")) 0 (-1)
      else L.runKVDB connName $ L.smembers (enc (setKeyFor "gDel"))
  -- SET delete keeps a dead tombstone at the pkey (membership lingers); ZSET delete ZREMs.
  ok6 <-
    check "ZSET delete removes member (SET path keeps dead-tombstone membership)" $
      if expectZ then afterDel == Right [] else True

  -- ---- 7. multi-cloud union of secondary-key resolution ------------------
  putStrLn "\n[7] multi-cloud: secondary-key resolution unions pkeys across both clouds"
  cleanPg
  r7 <- runFlow $ do
    let g = "gMC"; rowA = mkRow "mcA" g 100; rowB = mkRow "mcB" g 200
    _ <- createWithKVConnector dbConf cfgPrimaryOnly rowA
    _ <- createWithKVConnector dbConf cfgSecondaryAsPrimary rowB
    res <- getPrimaryKeyFromFieldsAndValues "kvSorted" cfgMultiCloud (shouldReadZSet mode True) (keyMap @KVSorted) [("groupId", g)]
    pure (either (const Nothing) (Just . sort) res)
  ok7 <- check "union == {mcA, mcB} across primary+secondary" (r7 == Just (sort [pkOf (mkRow "mcA" "gMC" 100), pkOf (mkRow "mcB" "gMC" 200)]))

  -- ---- 8. In on secondary key — multiple combos ⇒ fast-path falls back, union ----
  putStrLn "\n[8] In on secondary key (groupId IN [gA,gB]) unions + orders correctly"
  cleanPg
  r8 <- runFlow $ do
    forM_ [mkRow "ia1" "gA" 100, mkRow "ia2" "gA" 300, mkRow "ib1" "gB" 200] (createWithKVConnector dbConf meshCfg)
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (In (["gA", "gB"] :: [Text]))] (Desc updatedAt) (Just 10) Nothing
  ok8 <- check "In[gA,gB] desc updatedAt == [ia2, ib1, ia1]" $ case r8 of
    Right rows -> map id rows == ["ia2", "ib1", "ia1"]
    Left _ -> False

  -- ---- 9. residual non-indexed predicate ⇒ fast-path falls back, filter applied ----
  putStrLn "\n[9] residual predicate (groupId=g AND payload=..) filters in memory"
  cleanPg
  r9 <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg (mkRow "r1keep" "g9" 100) {payload = "keep"}
    _ <- createWithKVConnector dbConf meshCfg (mkRow "r2drop" "g9" 200) {payload = "drop"}
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("g9" :: Text)), Is payload (Eq ("keep" :: Text))] (Desc updatedAt) (Just 10) Nothing
  ok9 <- check "residual payload filter ⇒ [r1keep]" $ case r9 of
    Right rows -> map id rows == ["r1keep"]
    Left _ -> False

  -- ---- 10. orderBy on a non-score column ⇒ fast-path falls back, sorts by it ----
  putStrLn "\n[10] orderBy != score column (Desc payload) falls back, sorts by payload"
  cleanPg
  r10 <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg (mkRow "o_z" "g10" 100) {payload = "zzz"}
    _ <- createWithKVConnector dbConf meshCfg (mkRow "o_a" "g10" 300) {payload = "aaa"}
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("g10" :: Text))] (Desc payload) (Just 10) Nothing
  ok10 <- check "Desc payload ⇒ [o_z, o_a]" $ case r10 of
    Right rows -> map id rows == ["o_z", "o_a"]
    Left _ -> False

  -- ---- 11. offset + limit ----
  putStrLn "\n[11] offset 1 limit 1 (Desc updatedAt) ⇒ second row"
  cleanPg
  r11 <- runFlow $ do
    forM_ [mkRow "c1" "g11" 100, mkRow "c2" "g11" 200, mkRow "c3" "g11" 300] (createWithKVConnector dbConf meshCfg)
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("g11" :: Text))] (Desc updatedAt) (Just 1) (Just 1)
  ok11 <- check "offset 1 limit 1 ⇒ [c2]" $ case r11 of
    Right rows -> map id rows == ["c2"]
    Left _ -> False

  -- ---- 12. TTL-skew: a dead ZSET member (row blob gone) must not corrupt results ----
  putStrLn "\n[12] dead ZSET member (TTL-skew) ⇒ pushdown falls back, result still correct"
  cleanPg
  r12 <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg (mkRow "real" "g12" 100)
    -- inject a stale member (no row blob) with a HIGH score, as a TTL-expired row would leave
    _ <- L.runKVDB connName $ L.zadd (enc (zKeyFor "g12")) [(9.99e11, enc "kvSorted_id_ghost{shard-1}")]
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("g12" :: Text))] (Desc updatedAt) (Just 10) Nothing
  ok12 <- check "stale member skipped ⇒ [real]" $ case r12 of
    Right rows -> map id rows == ["real"]
    Left _ -> False

  -- ---- 13. empty result ----
  putStrLn "\n[13] no matching rows ⇒ []"
  cleanPg
  r13 <- runFlow $ findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("nope" :: Text))] (Desc updatedAt) (Just 10) Nothing
  ok13 <- check "empty ⇒ []" $ case r13 of Right [] -> True; _ -> False

  -- ---- 14. In on PRIMARY key (batch get) ----
  putStrLn "\n[14] In on primary key (id IN [..]) batch get + order"
  cleanPg
  r14 <- runFlow $ do
    forM_ [mkRow "p1" "g14" 100, mkRow "p2" "g14" 200, mkRow "p3" "g14" 300] (createWithKVConnector dbConf meshCfg)
    findAllWithOptionsKVConnector dbConf meshCfg [Is id (In (["p1", "p2", "p3"] :: [Text]))] (Desc updatedAt) (Just 10) Nothing
  ok14 <- check "id IN [p1,p2,p3] desc ⇒ [p3,p2,p1]" $ case r14 of
    Right rows -> map id rows == ["p3", "p2", "p1"]
    Left _ -> False

  -- ---- 15. dedup: a row present in both clouds is returned exactly once ----
  putStrLn "\n[15] dedup — a row in BOTH clouds is returned once (multi-cloud findAllWithOptions)"
  cleanPg
  r15 <- runFlow $ do
    let row = mkRow "dup1" "g15" 100
    _ <- createWithKVConnector dbConf cfgPrimaryOnly row
    _ <- createWithKVConnector dbConf cfgSecondaryAsPrimary row
    findAllWithOptionsKVConnector dbConf cfgMultiCloud [Is groupId (Eq ("g15" :: Text))] (Desc updatedAt) (Just 10) Nothing
  ok15 <- check "row in both clouds appears once" $ case r15 of
    Right rows -> map id rows == ["dup1"]
    Left _ -> False

  -- ---- 16. range query (non-Eq/In term) ⇒ no KV key ⇒ served from Postgres ----
  putStrLn "\n[16] range query (updatedAt > t) — no KV index key ⇒ Postgres fallback"
  cleanPg
  runRaw "INSERT INTO kv_sorted (id,group_id,updated_at,payload) VALUES ('rg1','gR','2026-03-01T00:00:00Z','x'),('rg2','gR','2026-03-02T00:00:00Z','y')"
  r16 <- runFlow $ findAllWithOptionsKVConnector dbConf meshCfg [Is updatedAt (GreaterThan (UTCTime (fromGregorian 2026 2 28) (secondsToDiffTime 0)))] (Desc updatedAt) (Just 10) Nothing
  ok16 <- check "range > t ⇒ [rg2, rg1] from DB" $ case r16 of
    Right rows -> map id rows == ["rg2", "rg1"]
    Left _ -> False

  -- ---- 17. OR across two groups ⇒ fast-path falls back, union returned ----
  putStrLn "\n[17] OR of two groups ⇒ fast-path falls back, union returned"
  cleanPg
  r17 <- runFlow $ do
    _ <- createWithKVConnector dbConf meshCfg (mkRow "or_a" "g17a" 100)
    _ <- createWithKVConnector dbConf meshCfg (mkRow "or_b" "g17b" 200)
    findAllWithOptionsKVConnector dbConf meshCfg [Or [Is groupId (Eq ("g17a" :: Text)), Is groupId (Eq ("g17b" :: Text))]] (Desc updatedAt) (Just 10) Nothing
  ok17 <- check "OR[g17a,g17b] ⇒ [or_b, or_a]" $ case r17 of
    Right rows -> map id rows == ["or_b", "or_a"]
    Left _ -> False

  -- ---- 18. ZSET fast-path is OBSERVABLE: the pushdown reads the ZSET only (no
  -- DB union), the in-mem fallback unions Postgres. A DB-only row with the highest
  -- score therefore appears in the result ONLY when the fallback ran. So in
  -- ZSetOnly the pushdown must EXCLUDE it; in SetOnly/DualWrite the fallback must
  -- INCLUDE it. This makes a dead pushdown (always Nothing) detectable: it would
  -- include the DB row in ZSetOnly and fail. ----
  putStrLn "\n[18] ZSET fast-path is observable (pushdown is index-only; fallback unions DB)"
  cleanPg
  r18 <- runFlow $ do
    -- two KV rows (go into the ZSET in ZSetOnly), scores 100 & 200
    _ <- createWithKVConnector dbConf meshCfg (mkRow "fp_lo" "gFP" 100)
    _ <- createWithKVConnector dbConf meshCfg (mkRow "fp_hi" "gFP" 200)
    -- a DB-ONLY row with the HIGHEST score (300) — present in Postgres, absent from the ZSET
    L.runIO $ runRaw "INSERT INTO kv_sorted (id,group_id,updated_at,payload) VALUES ('fp_db','gFP','2026-01-01T00:05:00Z','db')"
    findAllWithOptionsKVConnector dbConf meshCfg [Is groupId (Eq ("gFP" :: Text))] (Desc updatedAt) (Just 2) Nothing
  let gotIds = case r18 of Right rows -> map id rows; Left _ -> []
      pushdownRan = shouldReadZSet mode True -- True only in ZSetOnly for a sorted table
      expected = if pushdownRan then ["fp_hi", "fp_lo"] else ["fp_db", "fp_hi"]
  ok18 <-
    check
      ( "fast-path "
          <> (if pushdownRan then "pushdown ⇒ DB-only high-score row EXCLUDED ⇒ [fp_hi,fp_lo]" else "fallback ⇒ DB row UNIONED ⇒ [fp_db,fp_hi]")
      )
      (gotIds == expected)

  -- ---- 19. MIXED-VERSION SAFETY (rolling deploy / two binaries) ----
  -- The phase now lives on MeshConfig (per-table, from shared-kernel's Tables
  -- config), so we can prove cross-version coexistence in ONE process by reading
  -- the same DualWrite-written row through three different configs:
  --   * a DualWrite WRITE populates BOTH the SET and the ZSET;
  --   * a SetOnly  reader (an OLD pod, or a not-yet-cut-over new pod) reads the SET → finds it;
  --   * a ZSetOnly reader (a fully cut-over new pod)                 reads the ZSET → finds it.
  -- This is the guarantee that lets an old (no-sorted-set) deployment and a new
  -- one run side by side during the DualWrite phase without losing rows.
  putStrLn "\n[19] mixed-version: a DualWrite-written row is found by BOTH a SET reader and a ZSET reader"
  cleanPg
  let cfgDual = meshCfg {sortedIndexMode = DualWrite}
      cfgSet = meshCfg {sortedIndexMode = SetOnly}
      cfgZ = meshCfg {sortedIndexMode = ZSetOnly}
  (viaSet, viaZ) <- runFlow $ do
    flushTestKeys
    _ <- createWithKVConnector dbConf cfgDual (mkRow "mv_a" "gMV" 100) -- writes SET + ZSET
    _ <- createWithKVConnector dbConf cfgDual (mkRow "mv_b" "gMV" 200)
    a <- findAllWithOptionsKVConnector dbConf cfgSet [Is groupId (Eq ("gMV" :: Text))] (Desc updatedAt) (Just 10) Nothing
    b <- findAllWithOptionsKVConnector dbConf cfgZ [Is groupId (Eq ("gMV" :: Text))] (Desc updatedAt) (Just 10) Nothing
    pure (a, b)
  ok19a <- check "old/SET reader sees DualWrite rows ⇒ [mv_b, mv_a]" $ case viaSet of Right rs -> map id rs == ["mv_b", "mv_a"]; Left _ -> False
  ok19b <- check "new/ZSET reader sees DualWrite rows ⇒ [mv_b, mv_a]" $ case viaZ of Right rs -> map id rs == ["mv_b", "mv_a"]; Left _ -> False

  cleanPg
  let results = [ok1, ok2a, ok2b, ok3, ok4, ok5a, ok5b, ok6, ok7, ok8, ok9, ok10, ok11, ok12, ok13, ok14, ok15, ok16, ok17, ok18, ok19a, ok19b]
      passed = length (filter (== True) results)
  putStrLn $ "\n=== [phase=" <> show mode <> "] " <> show passed <> "/" <> show (length results) <> " passed ==="
  pure (and results)
