{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module EulerHS.KVConnector.Utils where

import qualified Data.Aeson as A
import Data.Aeson.Encode.Pretty (encodePretty)
import qualified Data.Aeson.Key as AKey
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.ByteString.Lazy as BSL
-- import           Servant (err500)

import Data.Either.Extra (mapLeft, mapRight)
import qualified Data.Fixed as Fixed
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.List (findIndices, intersect)
import qualified Data.List as DL
import qualified Data.Map.Strict as SMap
import qualified Data.Maybe as DM
import qualified Data.Serialize as Cereal
import qualified Data.Serialize as Serialize
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Time.Clock (secondsToNominalDiffTime)
import Data.Time.LocalTime (LocalTime, addLocalTime)
import qualified Database.Beam as B
import qualified Database.Beam.Schema.Tables as B
import qualified Database.Redis as DR
import EulerHS.KVConnector.Encoding ()
import qualified EulerHS.KVConnector.Encoding as Encoding
import EulerHS.KVConnector.Metrics (KVMetric (..), incrementMetric, observeSecondaryKeyElements)
import EulerHS.KVConnector.Types
  ( DBCommandVersion (..),
    DBLogEntry (..),
    KVConnector (..),
    MerchantID (..),
    MeshConfig (..),
    MeshError (..),
    MeshMeta (..),
    MeshResult,
    Operation (..),
    PartialCond (..),
    PrimaryKey (..),
    SecondaryKey (..),
    SecondaryKeyConfig (..),
    SecondaryKeyExpiry (..),
    Source (..),
  )
import EulerHS.KVDB.Types (KVDBReply)
import qualified EulerHS.Language as L
import qualified EulerHS.Logger.Types as Log
import EulerHS.Prelude
import EulerHS.SqlDB.Types (DBError (..), DBErrorType (..))
import EulerHS.Types (ApiTag (..))
import Juspay.Extra.Config (lookupEnvT)
import Safe (atMay)
import Sequelize (Clause (..), Model, Set (..), Term (..), Where, columnize, fromColumnar', modelTableName)
import Sequelize.SQLObject (ToSQLObject (..))
import System.Random (randomRIO)
import Text.Casing (quietSnake)
import Unsafe.Coerce (unsafeCoerce)

jsonKeyValueUpdates ::
  forall be table.
  (HasCallStack, Model be table, MeshMeta be table) =>
  DBCommandVersion ->
  [Set be table] ->
  [(Text, A.Value)]
jsonKeyValueUpdates version = fmap (jsonSet version)

jsonSet ::
  forall be table.
  (HasCallStack, Model be table, MeshMeta be table) =>
  DBCommandVersion ->
  Set be table ->
  (Text, A.Value)
jsonSet V1 (Set column value) = (key, modifiedValue)
  where
    key =
      B._fieldName . fromColumnar' . column . columnize $
        B.dbTableSettings (meshModelTableEntityDescriptor @table @be)
    modifiedValue = A.toJSON value
jsonSet V2 (Set column value) = (key, modifiedValue)
  where
    key =
      B._fieldName . fromColumnar' . column . columnize $
        B.dbTableSettings (meshModelTableEntityDescriptor @table @be)
    modifiedValue = A.toJSON . convertToSQLObject $ value
jsonSet _ (SetDefault _) = error "Default values are not supported"

-- | Update the model by setting it's fields according the given
--   key value mapping.
updateModel ::
  forall be table.
  ( MeshMeta be table,
    ToJSON (table Identity)
  ) =>
  table Identity ->
  [(Text, A.Value)] ->
  MeshResult A.Value
updateModel model updVals = do
  let updVals' = map (\(key, v) -> (AKey.fromText key, SMap.findWithDefault id key (valueMapper @be @table) v)) updVals
  case A.toJSON model of
    A.Object o -> Right (A.Object $ foldr (uncurry AKM.insert) o updVals')
    o ->
      Left $
        MUpdateFailed
          ( "Failed to update a model. Expected a JSON object but got '"
              <> (decodeUtf8 . BSL.toStrict . encodePretty $ o)
              <> "'."
          )

getDataFromRedisForPKey ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  MeshConfig ->
  Text ->
  m (MeshResult (Maybe (Text, Bool, table Identity)))
getDataFromRedisForPKey meshCfg pKey = do
  res <- L.runKVDB meshCfg.kvRedis $ L.get (fromString $ T.unpack $ pKey)
  case res of
    Right (Just r) ->
      let (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
       in case decodeResult of
            Right [decodeRes] -> return . Right . Just $ (pKey, isLive, decodeRes)
            Right _ -> return . Right $ Nothing -- Something went wrong
            Left e -> return $ Left e
    Right Nothing -> do
      let traceMsg = "redis_fetch_noexist: Could not find key: " <> show pKey
      L.logWarningT "getCacheWithHash" traceMsg
      return $ Right Nothing
    Left e -> return $ Left $ RedisError $ (show e <> " for key: " <> show pKey <> " in getDataFromRedisForPKey")

slotMap :: SMap.Map DR.HashSlot [ByteString]
slotMap = SMap.empty

groupKeysBySlot :: [ByteString] -> [[ByteString]]
groupKeysBySlot keys' =
  let slotMap' = slotMap
      result = foldl' (\acc key -> insertKeyIntoMap key acc) slotMap' keys'
   in SMap.elems result
  where
    insertKeyIntoMap key acc =
      let slot = L.keyToSlot key -- Assuming L contains the keyToSlot function
          slotMap'' =
            if keyExists slot acc
              then insertedMap slot (key : valuesForKey slot acc) acc
              else insertedMap slot [key] acc
       in slotMap''
      where
        keyExists = SMap.member
        valuesForKey slot acc' = DM.fromJust $ SMap.lookup slot acc'
        insertedMap = SMap.insert

getDataFromPKeysRedisHelper ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  Either KVDBReply [(Maybe ByteString)] ->
  m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedisHelper redisResult = do
  result <- processResults [] [] redisResult
  pure $ case result of
    Right (liveAcc, deadAcc) -> Right (reverse liveAcc, reverse deadAcc)
    Left e -> Left e
  where
    processResults liveAcc deadAcc (Right []) = pure $ Right (liveAcc, deadAcc)
    processResults liveAcc deadAcc (Right (Nothing : xs)) = processResults liveAcc deadAcc (Right xs)
    processResults liveAcc deadAcc (Right (Just r : xs)) = do
      let (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
      case decodeResult of
        Right decodeRes ->
          if isLive
            then processResults (decodeRes ++ liveAcc) deadAcc (Right xs)
            else processResults liveAcc (decodeRes ++ deadAcc) (Right xs)
        Left e -> return $ Left e
    processResults _ _ (Left e) = return $ Left $ MRedisError e

getDataFromPKeysHelper ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  MeshConfig ->
  [[ByteString]] ->
  m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysHelper meshCfg pKeysList = do
  results <- mapM processKeyGroup pKeysList
  pure $ case sequence results of
    Right pairs ->
      let (allLive, allDead) = unzip pairs
       in Right (concat allLive, concat allDead)
    Left e -> Left e
  where
    processKeyGroup pKey = do
      res <- L.runKVDB meshCfg.kvRedis $ L.mget (fromString . T.unpack . decodeUtf8 <$> pKey)
      getDataFromPKeysRedisHelper res

getDataFromPKeysHelperAsync ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  MeshConfig ->
  [[ByteString]] ->
  m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysHelperAsync _ [] = pure $ Right ([], [])
getDataFromPKeysHelperAsync meshCfg pKeysList = do
  resList <- mapM callAsyncMget pKeysList
  results <- awaitAll resList
  processResults results
  where
    callAsyncMget pKeys =
      L.awaitableFork $
        L.runKVDB meshCfg.kvRedis $
          L.mget (fromString . T.unpack . decodeUtf8 <$> pKeys)

    awaitAll = mapM (L.await Nothing)

    processResults results = do
      pairs <- mapM processResult results
      pure $ case sequence pairs of
        Right allPairs ->
          let (allLive, allDead) = unzip allPairs
           in Right (concat allLive, concat allDead)
        Left e -> Left e

    processResult res = case res of
      Left e -> pure $ Left (RedisPipelineError (show e))
      Right redisRes -> getDataFromPKeysRedisHelper redisRes

getDataFromPKeysRedis' ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  MeshConfig ->
  [ByteString] ->
  m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedis' _ [] = pure $ Right ([], [])
getDataFromPKeysRedis' meshCfg pKeys = do
  let groupedKeys = groupKeysBySlot pKeys
      (startShard, endShard) = meshCfg.tableShardModRange
  if abs (endShard - startShard) <= 20 -- to avoid the parallelism overhead
    then getDataFromPKeysHelperAsync meshCfg groupedKeys
    else getDataFromPKeysHelper meshCfg groupedKeys

getDataFromPKeysRedis ::
  forall table m.
  ( KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m
  ) =>
  Text ->
  [ByteString] ->
  m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedis _ [] = pure $ Right ([], [])
getDataFromPKeysRedis redisConn (pKey : pKeys) = do
  res <- L.runKVDB redisConn $ L.get (fromString $ T.unpack $ decodeUtf8 pKey)
  case res of
    Right (Just r) -> do
      let (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
      case decodeResult of
        Right decodeRes -> do
          remainingPKeysResult <- getDataFromPKeysRedis redisConn pKeys
          case remainingPKeysResult of
            Right remainingResult -> do
              if isLive
                then return $ Right (decodeRes ++ (fst remainingResult), snd remainingResult)
                else return $ Right (fst remainingResult, decodeRes ++ (snd remainingResult))
            Left err -> return $ Left err
        Left e -> do
          -- to handle the case where the key is not found in the redis and log the error
          L.logErrorT "getDataFromPKeysRedis" $ "Error while decoding: " <> show e
          return $ Right ([], [])
    Right Nothing -> getDataFromPKeysRedis redisConn pKeys
    Left e -> return $ Left $ RedisError $ (show e <> " for key: " <> show (pKey : pKeys))

------------- KEY UTILS ------------------

keyDelim :: Text
keyDelim = "_"

getPKeyWithShard :: forall table. (KVConnector (table Identity)) => table Identity -> Text -> (Int, Int) -> Text
getPKeyWithShard table redisKeyPrefix tableShardModRange =
  let pKey = getLookupKeyByPKey redisKeyPrefix table
   in pKey <> getShardedHashTag tableShardModRange pKey

getLookupKeyByPKey :: forall table. (KVConnector (table Identity)) => Text -> table Identity -> Text
getLookupKeyByPKey redisKeyPrefix table = do
  let tName = tableName @(table Identity)
  let (PKey k) = primaryKey table
  let lookupKey = getSortedKey k
  redisKeyPrefix <> tName <> keyDelim <> lookupKey

getSecondaryLookupKeys :: forall table. (KVConnector (table Identity)) => Text -> table Identity -> [Text]
getSecondaryLookupKeys redisKeyPrefix table = do
  let tName = tableName @(table Identity)
  let skeys = secondaryKeysFiltered table
  let tupList = map (\(SKey s) -> s) skeys
  let list = map (\x -> redisKeyPrefix <> tName <> keyDelim <> getSortedKey x) tupList
  list

secondaryKeysFiltered :: forall table. (KVConnector (table Identity)) => table Identity -> [SecondaryKey]
secondaryKeysFiltered table = filter filterEmptyValues (secondaryKeys table)
  where
    filterEmptyValues :: SecondaryKey -> Bool
    filterEmptyValues (SKey sKeyPairs) = not $ any (\p -> snd p == "") sKeyPairs

----------- Ordered secondary index helpers -----------

-- | Lower/upper score bounds used to mean "the whole sorted-set". Real scores
-- (epoch millis ~1.7e12, or numeric ids) fit comfortably within this range, so
-- we avoid relying on Redis "+inf"/"-inf" string serialisation.
skScoreNegInf :: Double
skScoreNegInf = -1.0e18

skScorePosInf :: Double
skScorePosInf = 1.0e18

-- | The sorted underscore-joined field-combo string identifying a secondary key.
--   Matches both 'keyMap'/'secondaryKeyConfigs' keys and TH's @sortAndGetKey@.
skeyFieldCombo :: [(Text, Text)] -> Text
skeyFieldCombo pairs = T.intercalate "_" $ sort $ map fst pairs

-- | Coerce a JSON field value to a ZSET score, if possible.
valueToScore :: A.Value -> Maybe Double
valueToScore = \case
  A.Number n -> Just (realToFrac n)
  A.Bool b -> Just (if b then 1 else 0)
  A.String s -> readMaybe (T.unpack s)
  _ -> Nothing

-- | Stringify a JSON value the same way secondary-key/clause values are.
jsonValToText :: A.Value -> Text
jsonValToText = \case
  A.String r -> r
  A.Number n -> T.pack $ show n
  A.Array l -> T.pack $ show l
  A.Object o -> T.pack $ show o
  A.Bool b -> T.pack $ show b
  A.Null -> ""

-- | Stable key suffix that distinguishes a partial index from the full index on
-- the same fields, so partial sets never collide with or pollute generic
-- lookups. Empty conditions => no suffix (full index).
partialSuffix :: [PartialCond] -> Text
partialSuffix [] = ""
partialSuffix conds = "_pf" <> T.concat (map condPart (sortOn pcField conds))
  where
    condPart (PartialCond f vs) = "_" <> f <> "_" <> T.intercalate "|" (sort vs)

-- | Does a row (as SQL JSON) satisfy all partial-index conditions?
--   NOTE: forces 'mkSQLObject'; callers keep it lazy for non-partial keys.
rowSatisfiesPartial :: A.Value -> [PartialCond] -> Bool
rowSatisfiesPartial rowJson conds = case rowJson of
  A.Object o -> all (condHolds o) conds
  _ -> False
  where
    condHolds o (PartialCond field vals) =
      case AKM.lookup (AKey.fromText field) o of
        Just v -> jsonValToText v `elem` vals
        Nothing -> False

-- | Compute the ZSET score for a row given the (optional) score field.
--   'Nothing' (or an uncoercible value) falls back to insertion time (millis).
--   NOTE: kept lazy in @rowJson@ so 'mkSQLObject' is only forced when a score
--   field is actually declared.
getScoreForRow :: A.Value -> Maybe Text -> Integer -> Double
getScoreForRow _ Nothing nowMillis = fromIntegral nowMillis
getScoreForRow rowJson (Just field) nowMillis =
  case rowJson of
    A.Object o -> case AKM.lookup (AKey.fromText field) o >>= valueToScore of
      Just s -> s
      Nothing -> fromIntegral nowMillis
    _ -> fromIntegral nowMillis

-- | Resolve a per-key expiry policy to a concrete TTL (seconds).
getSecondaryKeyTtl :: SecondaryKeyExpiry -> Integer -> Integer -> Integer
getSecondaryKeyTtl expiry defaultTtl nowMillis = case expiry of
  DefaultTTL -> defaultTtl
  TTLSeconds n -> n
  DailyAt h m ->
    let nowSec = nowMillis `div` 1000
        secOfDay = nowSec `mod` 86400
        target = toInteger (h * 3600 + m * 60)
        delta = target - secOfDay
     in if delta > 0 then delta else delta + 86400

-- | A fully-resolved secondary index entry to write for a row.
data SecondaryLookupKey = SecondaryLookupKey
  { slkKey :: Text, -- ^ full redis key string
    slkOrdered :: Bool, -- ^ store as ZSET (vs plain SET)
    slkScore :: Double, -- ^ ZSET score (valid when ordered)
    slkTtl :: Integer -- ^ ttl in seconds for this index
  }

-- | Like 'getSecondaryLookupKeys' but resolves each secondary key's config
--   (ordered?/score/ttl). @nowMillis@ is the write timestamp (epoch millis).
--   Honours the table-level KV config: skips any index whose field-combo is in
--   @meshCfg.disableSecondaryKeys@, and coerces every index to a plain SET
--   (@slkOrdered = False@) when @meshCfg.forceUnorderedSecondaryKeys@ is set.
getSecondaryLookupKeysWithConfig ::
  forall table.
  (KVConnector (table Identity)) =>
  MeshConfig ->
  Integer ->
  table Identity ->
  [SecondaryLookupKey]
getSecondaryLookupKeysWithConfig meshCfg nowMillis table =
  let redisKeyPrefix = meshCfg.redisKeyPrefix
      defaultTtl = meshCfg.redisTtl
      forceUnordered = meshCfg.forceUnorderedSecondaryKeys
      disabled = meshCfg.disableSecondaryKeys
      tName = tableName @(table Identity)
      cfgMap = secondaryKeyConfigs @(table Identity)
      rowJson = mkSQLObject table -- lazy: only forced for keys with a score field or partial predicate
   in DM.mapMaybe
        ( \(SKey pairs) ->
            let combo = skeyFieldCombo pairs
                mbCfg = HM.lookup combo cfgMap
                partial = maybe [] skPartial mbCfg
             in -- Skip disabled indexes, and partial indexes whose predicate the row does not satisfy.
                if combo `elem` disabled || (not (null partial) && not (rowSatisfiesPartial rowJson partial))
                  then Nothing
                  else
                    let keyStr = redisKeyPrefix <> tName <> keyDelim <> getSortedKey pairs <> partialSuffix partial
                        ordered = not forceUnordered && maybe False skOrdered mbCfg
                        score = getScoreForRow rowJson (mbCfg >>= skScoreField) nowMillis
                        ttl = maybe defaultTtl (\c -> getSecondaryKeyTtl (skExpiry c) defaultTtl nowMillis) mbCfg
                     in Just $ SecondaryLookupKey keyStr ordered score ttl
        )
        (secondaryKeysFiltered table)

applyFPair :: (t -> b) -> (t, t) -> (b, b)
applyFPair f (x, y) = (f x, f y)

getPKeyAndValueList :: forall table. (HasCallStack, KVConnector (table Identity), A.ToJSON (table Identity)) => table Identity -> [(Text, A.Value)]
getPKeyAndValueList table = do
  let (PKey k) = primaryKey table
      keyValueList = sortBy (compare `on` fst) k
      rowObject = mkSQLObject table
  case rowObject of
    A.Object hm -> DL.foldl' (\acc x -> (go hm x) : acc) [] keyValueList
    _ -> error "Cannot work on row that isn't an Object"
  where
    go hm x = case AKM.lookup (AKey.fromText $ fst x) hm of
      Just val -> (fst x, val)
      Nothing -> error $ "Cannot find " <> (fst x) <> " field in the row"

getSortedKey :: [(Text, Text)] -> Text
getSortedKey kvTup = do
  let sortArr = sortBy (compare `on` fst) kvTup
  let (appendedKeys, appendedValues) = applyFPair (T.intercalate "_") $ unzip sortArr
  appendedKeys <> "_" <> appendedValues

getShardedHashTag :: (Int, Int) -> Text -> Text
getShardedHashTag (start, modVal) key = do
  let slot = unsafeCoerce @_ @Word16 $ L.keyToSlot $ encodeUtf8 key
      streamShard = (fromIntegral start) + (slot `mod` (fromIntegral $ abs (modVal - start)))
  "{shard-" <> show streamShard <> "}"

------------------------------------------

getAutoIncId :: (L.MonadFlow m) => MeshConfig -> Text -> m (MeshResult Integer)
getAutoIncId meshCfg tName = do
  let key = (T.pack . quietSnake . T.unpack) tName <> "_auto_increment_id"
  mId <- L.runKVDB meshCfg.kvRedis $ L.incr $ encodeUtf8 key
  case mId of
    Right id_ -> return $ Right id_
    Left e -> return $ Left $ MRedisError e

unsafeJSONSetAutoIncId ::
  forall table m.
  (ToJSON (table Identity), FromJSON (table Identity), KVConnector (table Identity), L.MonadFlow m) =>
  MeshConfig ->
  table Identity ->
  m (MeshResult (table Identity))
unsafeJSONSetAutoIncId meshCfg obj = do
  let (PKey p) = primaryKey obj
  case p of
    [(field, _)] ->
      case A.toJSON obj of
        A.Object o -> do
          if AKM.member (AKey.fromText field) o
            then pure $ Right obj
            else do
              autoIncIdRes <- getAutoIncId meshCfg (tableName @(table Identity))
              case autoIncIdRes of
                Right value -> do
                  let jsonVal = A.toJSON value
                      newObj = A.Object (AKM.insert (AKey.fromText field) jsonVal o)
                  case resultToEither $ A.fromJSON newObj of
                    Right r -> pure $ Right r
                    Left e -> pure $ Left $ MDecodingError (show e)
                Left err -> pure $ Left err
        _ -> pure $ Left $ MDecodingError "Can't set AutoIncId value of JSON which isn't a object."
    _ -> pure $ Right obj

foldEither :: [Either a b] -> Either a [b]
foldEither [] = Right []
foldEither ((Left a) : _) = Left a
foldEither ((Right b) : xs) = mapRight ((:) b) (foldEither xs)

resultToEither :: A.Result a -> Either Text a
resultToEither (A.Success res) = Right res
resultToEither (A.Error e) = Left $ T.pack e

mergeKVAndDBResults :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
mergeKVAndDBResults redisKeyPrefix dbRows kvRows = do
  let kvPkeys = map (getLookupKeyByPKey redisKeyPrefix) kvRows
      uniqueDbRes = filter (\r -> (getLookupKeyByPKey redisKeyPrefix r) `notElem` kvPkeys) dbRows
  kvRows ++ uniqueDbRes

getUniqueDBRes :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
getUniqueDBRes redisKeyPrefix dbRows kvRows = do
  let kvPkeysSet = HS.fromList $ map (getLookupKeyByPKey redisKeyPrefix) kvRows
  filter (\r -> not $ HS.member (getLookupKeyByPKey redisKeyPrefix r) kvPkeysSet) dbRows

removeDeleteResults :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
removeDeleteResults redisKeyPrefix delRows rows = do
  let delPKeysSet = HS.fromList $ map (getLookupKeyByPKey redisKeyPrefix) delRows
      nonDelRows = filter (\r -> not $ HS.member (getLookupKeyByPKey redisKeyPrefix r) delPKeysSet) rows
  nonDelRows

getLatencyInMicroSeconds :: Integer -> Integer
getLatencyInMicroSeconds execTime = execTime `div` 1000000

---------------- Match where clauses -------------
findOneMatching :: B.Beamable table => Where be table -> [table Identity] -> Maybe (table Identity)
findOneMatching whereClause = find (`matchWhereClause` whereClause)

findAllMatching :: B.Beamable table => Where be table -> [table Identity] -> [table Identity]
findAllMatching whereClause = filter (`matchWhereClause` whereClause)

-- Helper function to match and deduplicate rows from both Redis instances
matchAndDeduplicateKVRows :: (KVConnector (table Identity), B.Beamable table) => MeshConfig -> Where be table -> [table Identity] -> [table Identity] -> [table Identity]
matchAndDeduplicateKVRows meshCfg whereClause primaryKvLiveRows secondaryKvLiveRows =
  let primaryMatchedKVLiveRows = findAllMatching whereClause primaryKvLiveRows
      secondaryMatchedKVLiveRows = findAllMatching whereClause secondaryKvLiveRows
      uniqueSecondaryRows = getUniqueDBRes meshCfg.redisKeyPrefix secondaryMatchedKVLiveRows primaryMatchedKVLiveRows
   in primaryMatchedKVLiveRows ++ uniqueSecondaryRows

-- Helper function to deduplicate dead rows from both Redis instances
deduplicateKVDeadRows :: (KVConnector (table Identity)) => MeshConfig -> [table Identity] -> [table Identity] -> [table Identity]
deduplicateKVDeadRows meshCfg primaryKvDeadRows secondaryKvDeadRows =
  let uniqueSecondaryDeadRows = getUniqueDBRes meshCfg.redisKeyPrefix secondaryKvDeadRows primaryKvDeadRows
   in primaryKvDeadRows ++ uniqueSecondaryDeadRows

-- Helper function to create configs for multi-cloud Redis queries
createMultiCloudConfigs :: MeshConfig -> (MeshConfig, MeshConfig)
createMultiCloudConfigs meshCfg =
  let primaryOnlyCfg = meshCfg {secondaryRedisEnabled = False}
      secondaryAsPrimaryCfg = meshCfg {kvRedis = meshCfg.kvRedisSecondary, secondaryRedisEnabled = False}
   in (primaryOnlyCfg, secondaryAsPrimaryCfg)

-- Helper function to extract and process multi-cloud Redis results
extractMultiCloudKVRows :: MeshResult ([table Identity], [table Identity]) -> MeshResult ([table Identity], [table Identity]) -> MeshResult ([table Identity], [table Identity], [table Identity], [table Identity])
extractMultiCloudKVRows primaryKvRows secondaryKvRows = case (primaryKvRows, secondaryKvRows) of
  (Right primaryKvRes, Right secondaryKvRes) -> Right (fst primaryKvRes, snd primaryKvRes, fst secondaryKvRes, snd secondaryKvRes)
  (Left err, _) -> Left err
  (_, Left err) -> Left err

-- Helper function to run two async operations in parallel
callKVKVAsync :: (L.MonadFlow m) => m (MeshResult a) -> m (MeshResult b) -> m (MeshResult a, MeshResult b)
callKVKVAsync action1 action2 = do
  -- Fork both actions
  awaitable1 <- L.awaitableFork action1
  awaitable2 <- L.awaitableFork action2

  -- Wait for all results
  res1 <- L.await Nothing awaitable1
  res2 <- L.await Nothing awaitable2

  -- Handle results (convert errors to MeshResult format)
  let res1' = case res1 of
        Right r -> r
        Left err -> Left $ AsyncKVCallFailed (show err)
      res2' = case res2 of
        Right r -> r
        Left err -> Left $ AsyncKVCallFailed (show err)

  pure (res1', res2')

-- Helper function to run three async operations in parallel
callMultiAsync :: (L.MonadFlow m) => m (MeshResult a) -> m (MeshResult b) -> m (Either DBError c) -> m (MeshResult a, MeshResult b, Either DBError c)
callMultiAsync action1 action2 action3 = do
  -- Fork all three actions
  awaitable1 <- L.awaitableFork action1
  awaitable2 <- L.awaitableFork action2
  awaitable3 <- L.awaitableFork action3

  -- Wait for all results
  res1 <- L.await Nothing awaitable1
  res2 <- L.await Nothing awaitable2
  res3 <- L.await Nothing awaitable3

  -- Handle results (convert errors to MeshResult/DBError format)
  let res1' = case res1 of
        Right r -> r
        Left err -> Left $ AsyncKVCallFailed (show err)
      res2' = case res2 of
        Right r -> r
        Left err -> Left $ AsyncKVCallFailed (show err)
      res3' = case res3 of
        Right r -> r
        Left err -> Left (DBError AsyncDBCallFailed (show err))

  pure (res1', res2', res3')

matchWhereClause :: B.Beamable table => table Identity -> [Clause be table] -> Bool
matchWhereClause row = all matchClauseQuery
  where
    matchClauseQuery = \case
      And queries -> all matchClauseQuery queries
      Or queries -> any matchClauseQuery queries
      Is column' term ->
        let column = fromColumnar' . column' . columnize
         in termQueryMatch (column row) term

termQueryMatch :: (Ord value, ToJSON value) => value -> Term be value -> Bool
termQueryMatch columnVal = \case
  In literals -> any (matchWithCaseInsensitive columnVal) literals
  Null -> isNothing columnVal
  Eq literal -> matchWithCaseInsensitive columnVal literal
  GreaterThan literal -> columnVal > literal
  GreaterThanOrEq literal -> columnVal >= literal
  LessThan literal -> columnVal < literal
  LessThanOrEq literal -> columnVal <= literal
  Not Null -> isJust columnVal
  Not (Eq literal) -> not $ matchWithCaseInsensitive columnVal literal
  Not term -> not (termQueryMatch columnVal term)
  _ -> error "Term query not supported"
  where
    matchWithCaseInsensitive c1 c2 =
      if c1 == c2
        then True
        else -- Fallback to case insensitive check (DB supports this)
        case (toJSON c1, toJSON c2) of
          (A.String s1, A.String s2) -> T.toLower s1 == T.toLower s2
          _ -> c1 == c2

-- | | Helper function to filter out rows based on where clause
--  || Do not use this as this is not efficient
getFilteredWhereClause :: forall be table. (B.Beamable table) => [table Identity] -> [Clause be table] -> [Clause be table]
getFilteredWhereClause rows = map matchClauseQuery
  where
    matchClauseQuery :: Clause be table -> Clause be table
    matchClauseQuery = \case
      And queries -> And $ map matchClauseQuery queries
      Or queries -> Or $ map matchClauseQuery queries
      Is column' term ->
        case term of
          In literals ->
            let colHashMap = SMap.fromList $ map (\row -> (fromColumnar' . column' . columnize $ row, True)) rows
                filteredLiterals = filter (`SMap.notMember` colHashMap) literals
             in Is column' (In filteredLiterals)
          _ -> Is column' term

toPico :: Int -> Fixed.Pico
toPico value = Fixed.MkFixed $ ((toInteger value) * 1000000000000)

getStreamName :: String -> Text
getStreamName shard = getConfigStreamBasename <> "-" <> (T.pack shard) <> ""

getRandomStream :: (L.MonadFlow m) => m Text
getRandomStream = do
  streamShard <- L.runIO' "random shard" $ randomRIO (1, getConfigStreamMaxShards)
  return $ getStreamName (show streamShard)

getConfigStreamNames :: [Text]
getConfigStreamNames = fmap (\shardNo -> getStreamName (show shardNo)) [1 .. getConfigStreamMaxShards]

getConfigStreamBasename :: Text
getConfigStreamBasename = fromMaybe "ConfigStream" $ lookupEnvT "CONFIG_STREAM_BASE_NAME"

getConfigStreamMaxShards :: Int
getConfigStreamMaxShards = fromMaybe 20 $ readMaybe =<< lookupEnvT @String "CONFIG_STREAM_MAX_SHARDS"

getConfigStreamLooperDelayInSec :: Int
getConfigStreamLooperDelayInSec = fromMaybe 5 $ readMaybe =<< lookupEnvT @String "CONFIG_STREAM_LOOPER_DELAY_IN_SEC"

getConfigEntryTtlJitterInSeconds :: Int
getConfigEntryTtlJitterInSeconds = fromMaybe 5 $ readMaybe =<< lookupEnvT @String "CONFIG_STREAM_TTL_JITTER_IN_SEC"

getConfigEntryBaseTtlInSeconds :: Int
getConfigEntryBaseTtlInSeconds = fromMaybe 10 $ readMaybe =<< lookupEnvT @String "CONFIG_STREAM_BASE_TTL_IN_SEC"

getConfigEntryNewTtl :: (L.MonadFlow m) => m LocalTime
getConfigEntryNewTtl = do
  currentTime <- L.getCurrentTimeUTC
  let jitterInSec = getConfigEntryTtlJitterInSeconds
      baseTtlInSec = getConfigEntryBaseTtlInSeconds
  noise <- L.runIO' "random seconds" $ randomRIO (1, jitterInSec)
  return $ addLocalTime (secondsToNominalDiffTime $ toPico (baseTtlInSec + noise)) currentTime

threadDelayMilisec :: Integer -> IO ()
threadDelayMilisec ms = threadDelay $ fromIntegral ms * 1000

meshModelTableEntityDescriptor ::
  forall table be.
  (Model be table, MeshMeta be table) =>
  B.DatabaseEntityDescriptor be (B.TableEntity table)
meshModelTableEntityDescriptor = let B.DatabaseEntity x = (meshModelTableEntity @table) in x

meshModelTableEntity ::
  forall table be db.
  (Model be table, MeshMeta be table) =>
  B.DatabaseEntity be db (B.TableEntity table)
meshModelTableEntity =
  let B.EntityModification modification = B.modifyTableFields (meshModelFieldModification @be @table)
   in appEndo modification $ B.DatabaseEntity $ B.dbEntityAuto (modelTableName @table)

toPSJSON :: forall be table. MeshMeta be table => (Text, A.Value) -> (Text, A.Value)
toPSJSON (k, v) = (k, SMap.findWithDefault id k (valueMapper @be @table) v)

decodeToField :: forall a. (FromJSON a, Serialize.Serialize a) => BSL.ByteString -> (MeshResult [a], Bool)
decodeToField val =
  let decodeRes = Encoding.decodeLiveOrDead val
   in case decodeRes of
        (isLive, byteString) ->
          let decodedMeshResult =
                let (h, v) = BSL.splitAt 4 byteString
                 in case h of
                      "CBOR" -> case Cereal.decodeLazy v of
                        Right r' -> Right [r']
                        Left _ -> case Cereal.decodeLazy v of
                          Right r'' -> Right r''
                          Left _ -> case Cereal.decodeLazy v of
                            Right r''' -> decodeField @a r'''
                            Left err' -> Left $ MDecodingError $ T.pack err'
                      "JSON" ->
                        case A.eitherDecode v of
                          Right r' -> decodeField @a r'
                          Left e -> Left $ MDecodingError $ T.pack e
                      _ ->
                        case A.eitherDecode val of
                          Right r' -> decodeField @a r'
                          Left e -> Left $ MDecodingError $ T.pack e
           in (decodedMeshResult, isLive)

decodeField :: forall a. (FromJSON a, Serialize.Serialize a) => A.Value -> MeshResult [a]
decodeField o@(A.Object _) =
  case A.eitherDecode @a $ A.encode o of
    Right r -> return [r]
    Left e -> Left $ MDecodingError $ T.pack e
decodeField o@(A.Array _) =
  mapLeft (MDecodingError . T.pack) $
    A.eitherDecode @[a] $ A.encode o
decodeField o =
  Left $
    MDecodingError
      ("Expected list or object but got '" <> T.pack (show o) <> "'.")

getFieldsAndValuesFromClause ::
  forall table be.
  (Model be table, MeshMeta be table) =>
  B.DatabaseEntityDescriptor be (B.TableEntity table) ->
  Clause be table ->
  [[(Text, Text)]]
getFieldsAndValuesFromClause dt = \case
  And cs -> foldl' processAnd [[]] $ map (getFieldsAndValuesFromClause dt) cs
  Or cs -> processOr cs
  Is column (Eq val) -> do
    let key = B._fieldName . fromColumnar' . column . columnize $ B.dbTableSettings dt
    [[(key, showVal . snd $ (toPSJSON @be @table) (key, A.toJSON val))]]
  Is column (In vals) -> do
    let key = B._fieldName . fromColumnar' . column . columnize $ B.dbTableSettings dt
    map (\val -> [(key, showVal . snd $ (toPSJSON @be @table) (key, A.toJSON val))]) vals
  _ -> []
  where
    processAnd xs [] = xs
    processAnd [] ys = ys
    processAnd xs ys = [x ++ y | x <- xs, y <- ys]
    processOr xs = concatMap (getFieldsAndValuesFromClause dt) xs

    showVal res = case res of
      A.String r -> r
      A.Number n -> T.pack $ show n
      A.Array l -> T.pack $ show l
      A.Object o -> T.pack $ show o
      A.Bool b -> T.pack $ show b
      A.Null -> T.pack ""

getPrimaryKeyFromFieldsAndValues :: (L.MonadFlow m) => Text -> MeshConfig -> HM.HashMap Text Bool -> HM.HashMap Text SecondaryKeyConfig -> [(Text, Text)] -> m (MeshResult [ByteString])
getPrimaryKeyFromFieldsAndValues _ _ _ _ [] = pure $ Right []
getPrimaryKeyFromFieldsAndValues modelName meshCfg keyHashMap cfgMap fieldsAndValues = do
  res <- foldEither <$> mapM getPrimaryKeyFromFieldAndValueHelper fieldsAndValues
  pure $ mapRight (intersectList . catMaybes) res
  where
    -- Read all pkey members of a secondary index. Ordered indexes are ZSETs
    -- (ZRANGEBYSCORE over the full score range); plain indexes are SETs. During
    -- the SET->ZSET migration window a ZSET read may hit a legacy SET, so we
    -- fall back to SMEMBERS on error.
    readSkeyMembers conn ordered key =
      if ordered
        then do
          r <- L.runKVDB conn $ L.zrangebyscore key skScoreNegInf skScorePosInf
          case r of
            Right ms -> pure $ Right ms
            Left _ -> L.runKVDB conn $ L.smembers key
        else L.runKVDB conn $ L.smembers key

    getPrimaryKeyFromFieldAndValueHelper (k, v) = do
      let constructedKey = meshCfg.redisKeyPrefix <> modelName <> "_" <> k <> "_" <> v
      case HM.lookup k keyHashMap of
        Just True -> pure $ Right $ Just [fromString $ T.unpack (constructedKey <> getShardedHashTag meshCfg.tableShardModRange constructedKey)]
        -- Disabled secondary index: skip Redis entirely so the lookup contributes
        -- no constraint and the query falls through to the DB.
        Just False | k `elem` meshCfg.disableSecondaryKeys -> pure $ Right Nothing
        Just False -> do
          let ordered = not meshCfg.forceUnorderedSecondaryKeys && maybe False skOrdered (HM.lookup k cfgMap)
              key = fromString $ T.unpack constructedKey
          -- Check primary Redis first
          primaryRes <- readSkeyMembers meshCfg.kvRedis ordered key
          case primaryRes of
            Left e -> pure $ Left $ RedisError $ (show e <> " for key: " <> show constructedKey)
            Right primaryKeys -> do
              observeSecondaryKeyElements modelName k "primaryCluster" (length primaryKeys)
              -- If secondary Redis is enabled, also check it and merge results
              if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
                then do
                  secondaryRes <- readSkeyMembers meshCfg.kvRedisSecondary ordered key
                  case secondaryRes of
                    Left e -> pure $ Left $ RedisError $ (show e <> " for secondary key: " <> show constructedKey)
                    Right secondaryKeys -> do
                      observeSecondaryKeyElements modelName k "secondaryCluster" (length secondaryKeys)
                      -- Merge primary keys from both Redis instances (union)
                      let mergedKeys = mkUniq (primaryKeys ++ secondaryKeys)
                      pure $ Right $ Just mergedKeys
                else pure $ Right $ Just primaryKeys
        _ -> pure $ Right Nothing

    intersectList (x : y : xs) = intersectList (intersect x y : xs)
    intersectList (x : []) = x
    intersectList [] = []

filterPrimaryAndSecondaryKeys :: HM.HashMap Text Bool -> [(Text, Text)] -> [(Text, Text)]
filterPrimaryAndSecondaryKeys keyHashMap fieldsAndValues = filter (\(k, _) -> HM.member k keyHashMap) fieldsAndValues

getSecondaryKeyLength :: HM.HashMap Text Bool -> [(Text, Text)] -> Int
getSecondaryKeyLength keyHashMap = length . filter (\(k, _) -> HM.lookup k keyHashMap == Just False)

mkUniq :: Ord a => [a] -> [a] -- O(n log n)
mkUniq = Set.toList . Set.fromList

-- >>> map (T.intercalate "_") (nonEmptySubsequences ["id", "id2", "id3"])
-- ["id","id2","id_id2","id3","id_id3","id2_id3","id_id2_id3"]
nonEmptySubsequences :: [Text] -> [[Text]]
nonEmptySubsequences [] = []
nonEmptySubsequences (x : xs) = [x] : foldr f [] (nonEmptySubsequences xs)
  where
    f ys r = ys : (x : ys) : r

whereClauseDiffCheck ::
  forall be table m.
  ( L.MonadFlow m,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity)
  ) =>
  Where be table ->
  m (Maybe [[Text]])
whereClauseDiffCheck whereClause =
  if isWhereClauseDiffCheckEnabled
    then do
      let keyAndValueCombinations = getFieldsAndValuesFromClause meshModelTableEntityDescriptor (And whereClause)
          andCombinations = map (uncurry zip . applyFPair (map (T.intercalate "_") . sortOn (Down . length) . nonEmptySubsequences) . unzip . sort) keyAndValueCombinations
          keyHashMap = keyMap @(table Identity)
          failedKeys = catMaybes $ map (atMay keyAndValueCombinations) $ findIndices (checkForPrimaryOrSecondary keyHashMap) andCombinations
      if (not $ null failedKeys)
        then do
          let diffRes = map (map fst) failedKeys
          if null $ concat diffRes
            then pure Nothing
            else L.logInfoT "WHERE_DIFF_CHECK" (tableName @(table Identity) <> ": " <> show diffRes) $> Just diffRes
        else pure Nothing
    else pure Nothing
  where
    checkForPrimaryOrSecondary _ [] = True
    checkForPrimaryOrSecondary keyHashMap ((k, _) : xs) =
      case HM.member k keyHashMap of
        True -> False
        _ -> checkForPrimaryOrSecondary keyHashMap xs

isWhereClauseDiffCheckEnabled :: Bool
isWhereClauseDiffCheckEnabled = fromMaybe True $ readMaybe =<< lookupEnvT @String "IS_WHERE_CLAUSE_DIFF_CHECK_ENABLED"

isRecachingEnabled :: Bool
isRecachingEnabled = fromMaybe False $ readMaybe =<< lookupEnvT @String "IS_RECACHING_ENABLED"

isCachingDbFindEnabled :: Bool
isCachingDbFindEnabled = fromMaybe False $ readMaybe =<< lookupEnvT @String "IS_CACHING_DB_FIND_ENABLED"

isPipeliningEnabled :: Bool
isPipeliningEnabled = fromMaybe True $ readMaybe =<< lookupEnvT @String "enablePipelining"

shouldLogFindDBCallLogs :: Bool
shouldLogFindDBCallLogs = fromMaybe False $ readMaybe =<< lookupEnvT @String "IS_FIND_DB_LOGS_ENABLED"

redisCallsHardLimit :: Int
redisCallsHardLimit = fromMaybe 5000 $ readMaybe =<< lookupEnvT @String "REDIS_CALLS_HARD_LIMIT"

redisCallsSoftLimit :: Int
redisCallsSoftLimit = fromMaybe 2500 $ readMaybe =<< lookupEnvT @String "REDIS_CALLS_SOFT_LIMIT"

lengthOfLists :: [[a]] -> Int
lengthOfLists = foldl' (\acc el -> acc + length el) 0

isLogsEnabledForModel :: Text -> Bool
isLogsEnabledForModel modelName = do
  let env :: Text = fromMaybe "development" $ lookupEnvT "NODE_ENV"
  if env == "production"
    then do
      let enableModelList = fromMaybe [] $ readMaybe =<< lookupEnvT @String "IS_LOGS_ENABLED_FOR_MODEL"
      modelName `elem` enableModelList
    else True

logAndIncrementKVMetric :: (L.MonadFlow m, ToJSON a) => Bool -> Text -> Operation -> MeshResult a -> Int -> Text -> Integer -> Source -> Maybe [[Text]] -> m ()
logAndIncrementKVMetric shouldLogData action operation res latency model cpuLatency source mbDiffCheckRes = do
  apiTag <- L.getOptionLocal ApiTag
  mid <- L.getOptionLocal MerchantID
  let shouldLogData_ = isLogsEnabledForModel model && shouldLogData
  let dblog =
        DBLogEntry
          { _log_type = "DB",
            _action = action, -- For logprocessor
            _operation = operation,
            _data = case res of
              Left err -> A.String (T.pack $ show err)
              Right m -> if shouldLogData_ then A.toJSON m else A.Null,
            _latency = latency,
            _model = model,
            _cpuLatency = getLatencyInMicroSeconds cpuLatency,
            _source = source,
            _apiTag = apiTag,
            _merchant_id = mid,
            _whereDiffCheckRes = mbDiffCheckRes
          }
  if action == "FIND"
    then when shouldLogFindDBCallLogs $ logDb Log.Debug ("DB" :: Text) source action model latency dblog
    else logDb Log.Info ("DB" :: Text) source action model latency dblog
  when (source == KV) $ L.setLoggerContext "PROCESSED_THROUGH_KV" "True"
  incrementMetric KVAction dblog (isLeft res)

logDb :: (L.MonadFlow m, ToJSON val) => Log.LogLevel -> Text -> Source -> Log.Action -> Log.Entity -> Int -> val -> m ()
logDb logLevel tag source action entity latency message =
  L.evalLogger' $ L.masterLogger logLevel tag category (Just action) (Just entity) Nothing (Just $ toInteger latency) Nothing $ Log.Message Nothing (Just $ A.toJSON message)
  where
    category
      | source == KV = "REDIS"
      | source == SQL = "DB"
      | source == KV_AND_SQL = "REDIS_AND_DB"
      | source == IN_MEM = "INMEM"
      | otherwise = ""
