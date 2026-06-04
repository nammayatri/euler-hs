{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-star-is-type #-}

module EulerHS.KVConnector.Types
  ( module EulerHS.KVConnector.Types,
    MeshError (..),
    TableMappings (..),
  )
where

import Data.Aeson ((.=))
import qualified Data.Aeson as A
import Data.Aeson.Types (Parser)
import Data.Data (Data)
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as Map
import Data.Time (Day, LocalTime, UTCTime, localTimeToUTC, toModifiedJulianDay, utc)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import qualified Database.Beam as B
import Database.Beam.Backend (BeamSqlBackend, HasSqlValueSyntax (sqlValueSyntax), autoSqlValueSyntax)
import qualified Database.Beam.Backend.SQL as B
import Database.Beam.MySQL (MySQL)
import Database.Beam.Schema (FieldModification, TableField)
import qualified EulerHS.KVDB.Language as L
import EulerHS.Prelude
import qualified EulerHS.Types as T
import Sequelize (Column, Set)
import Sequelize.SQLObject (ToSQLObject)

------------ TYPES AND CLASSES ------------

data DBCommandVersion = V1 | V2
  deriving (Generic, Show, ToJSON, FromJSON)

data PrimaryKey = PKey [(Text, Text)]

data SecondaryKey = SKey [(Text, Text)]

class KVConnector table where
  tableName :: Text
  keyMap :: HM.HashMap Text Bool -- True implies it is primary key and False implies secondary
  primaryKey :: table -> PrimaryKey
  secondaryKeys :: table -> [SecondaryKey]
  mkSQLObject :: table -> A.Value

  -- | Sorted-secondary-index support.
  --   'Nothing'  => secondary indexes are plain Redis SETs (default / legacy behaviour).
  --   'Just f'   => secondary indexes are maintained as sorted sets (ZSET) and @f@
  --                 extracts the score (epoch-millis / day-number) from a row.
  --   Populated by the @enableKVPGWithSortKey@ codegen macro; hand-written and
  --   non-sorted ('enableKVPG') instances inherit the 'Nothing' default, so this is
  --   fully backward compatible.
  sortScore :: Maybe (table -> Double)
  sortScore = Nothing

  -- | Name of the column the ZSET is scored by (e.g. @"updatedAt"@). Used by the read
  --   path to verify a query's @orderBy@ matches the ZSET ordering before pushing the
  --   LIMIT/OFFSET into Redis. 'Nothing' for non-sorted tables.
  sortScoreColumn :: Maybe Text
  sortScoreColumn = Nothing

----------------------------------------------

-- | Columns usable as a ZSET sort score must reduce to a monotonic 'Double'.
--   v1 supports time columns only. Equal scores tie-break by member (pkey) in Redis,
--   so ordering stays deterministic.
class ToZScore a where
  toZScore :: a -> Double

-- epoch milliseconds (fits a ZSET score's 52-bit double mantissa; do NOT use micros)
instance ToZScore UTCTime where
  toZScore = (* 1000) . realToFrac . utcTimeToPOSIXSeconds

-- treat LocalTime as UTC-naive for ordering purposes (consistent, not TZ-correct)
instance ToZScore LocalTime where
  toZScore = (* 1000) . realToFrac . utcTimeToPOSIXSeconds . localTimeToUTC utc

instance ToZScore Day where
  toZScore = fromIntegral . toModifiedJulianDay

----------------------------------------------

class TableMappings a where
  getTableMappings :: [(String, String)]
  getTableName :: Text

--------------- EXISTING DB MESH ---------------
class MeshState a where
  getShardedHashTag :: (Int, Int) -> a -> Maybe Text
  getKVKey :: a -> Maybe Text
  getKVDirtyKey :: a -> Maybe Text
  isDBMeshEnabled :: a -> Bool

class MeshMeta be table where
  meshModelFieldModification :: table (FieldModification (TableField table))
  valueMapper :: Map.Map Text (A.Value -> A.Value)
  parseFieldAndGetClause :: A.Value -> Text -> Parser (TermWrap be table)
  parseSetClause :: [(Text, A.Value)] -> Parser [Set be table]

data TermWrap be (table :: (* -> *) -> *) where
  TermWrap ::
    (B.BeamSqlBackendCanSerialize be a, A.ToJSON a, Ord a, B.HasSqlEqualityCheck be a, Show a, ToSQLObject a) =>
    Column table a ->
    a ->
    TermWrap be table

type MeshResult a = Either MeshError a

data MeshError
  = MKeyNotFound Text
  | MDBError T.DBError
  | MRedisError T.KVDBReply
  | RedisError Text
  | MDecodingError Text
  | MUpdateFailed Text
  | MMultipleKeysFound Text
  | UnexpectedError Text
  | RedisPipelineError Text
  | AsyncKVCallFailed Text
  deriving (Show, Generic, Exception, Data)

instance ToJSON MeshError where
  toJSON (MRedisError r) =
    A.object
      [ "contents" A..= (show r :: Text),
        "tag" A..= ("MRedisError" :: Text)
      ]
  toJSON a = A.toJSON a

data QueryPath = KVPath | SQLPath

-- | Rollout mode for sorted-secondary-indexes (ZSETs). Read from env
--   @KV_SORTED_INDEX_MODE@ (see 'EulerHS.KVConnector.Utils.sortedIndexMode').
--   Only affects tables that opted in via a non-'Nothing' 'sortScore'.
--
--   * 'SetOnly'   — legacy: secondary indexes are plain SETs only (default).
--   * 'DualWrite' — write BOTH SET and ZSET; reads still use the (complete) SET.
--                   Canary/warm-up phase: safe while old (SET-only) pods coexist.
--   * 'ZSetOnly'  — write/read ZSET only. Enter only after full rollout + >=1 TTL soak.
data SortedIndexMode = SetOnly | DualWrite | ZSetOnly
  deriving (Generic, Eq, Show, A.ToJSON, A.FromJSON)

data MeshConfig = MeshConfig
  { meshEnabled :: Bool,
    cerealEnabled :: Bool,
    memcacheEnabled :: Bool,
    meshDBName :: Text,
    ecRedisDBStream :: Text,
    kvRedis :: Text,
    kvRedisSecondary :: Text,
    secondaryRedisEnabled :: Bool,
    redisTtl :: L.KVDBDuration,
    kvHardKilled :: Bool,
    tableShardModRange :: (Int, Int),
    redisKeyPrefix :: Text,
    forceDrainToDB :: Bool,
    -- | Sorted-secondary-index (ZSET) rollout phase for THIS table. Set per-table
    --   in shared-kernel from the @Tables@ config (kv_configs), so the phase can be
    --   flipped centrally without a redeploy. Defaults to 'SetOnly' (legacy / safe);
    --   non-sorted tables ignore it.
    sortedIndexMode :: SortedIndexMode
  }
  deriving (Generic, Eq, Show, A.ToJSON)

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL UTCTime where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL UTCTime

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL A.Value where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL A.Value

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL (Vector Int) where
  sqlValueSyntax = autoSqlValueSyntax

instance HasSqlValueSyntax MySQL String => HasSqlValueSyntax MySQL (Vector Text) where
  sqlValueSyntax = autoSqlValueSyntax

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL (Vector Int)

instance BeamSqlBackend MySQL => B.HasSqlEqualityCheck MySQL (Vector Text)

data MerchantID = MerchantID
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

instance T.OptionEntity MerchantID Text

data Operation
  = CREATE
  | CREATE_RETURNING
  | UPDATE
  | UPDATE_RETURNING
  | UPDATE_ALL
  | UPDATE_ALL_RETURNING
  | FIND
  | FIND_ALL
  | FIND_ALL_WITH_OPTIONS
  | DELETE_ONE
  | DELETE_ONE_RETURNING
  | DELETE_ALL_RETURNING
  deriving (Generic, Show, ToJSON)

data Source = KV | SQL | KV_AND_SQL | IN_MEM
  deriving (Generic, Show, Eq, ToJSON)

data DBLogEntry a = DBLogEntry
  { _log_type :: Text,
    _action :: Text,
    _operation :: Operation,
    _data :: a,
    _latency :: Int,
    _model :: Text,
    _cpuLatency :: Integer,
    _source :: Source,
    _apiTag :: Maybe Text,
    _merchant_id :: Maybe Text,
    _whereDiffCheckRes :: Maybe [[Text]]
  }
  deriving stock (Generic)

-- deriving anyclass (ToJSON)
instance (ToJSON a) => ToJSON (DBLogEntry a) where
  toJSON val =
    A.object
      [ "log_type" .= _log_type val,
        "action" .= _action val,
        "operation" .= _operation val,
        "latency" .= _latency val,
        "model" .= _model val,
        "cpuLatency" .= _cpuLatency val,
        "data" .= _data val,
        "source" .= _source val,
        "api_tag" .= _apiTag val,
        "merchant_id" .= _merchant_id val,
        "whereDiffCheckRes" .= _whereDiffCheckRes val
      ]
