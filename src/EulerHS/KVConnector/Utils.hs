{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module EulerHS.KVConnector.Utils where

import           EulerHS.Prelude
import qualified Data.Aeson as A
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.Aeson.Key as AKey
import           Data.Aeson.Encode.Pretty (encodePretty)
import qualified Data.List as DL
import qualified Database.Beam as B
import qualified Database.Beam.Schema.Tables as B
import qualified Data.ByteString.Lazy as BSL
import           Text.Casing (quietSnake)
import qualified Data.HashMap.Strict as HM
import           Data.List (findIndices, intersect)
import qualified Data.Map.Strict as SMap
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified EulerHS.KVConnector.Encoding as Encoding
import           EulerHS.KVConnector.Metrics (incrementMetric, KVMetric(..))
import           EulerHS.KVConnector.Types (DBCommandVersion (..), MeshMeta(..), MeshResult, MeshError(..), MeshConfig, KVConnector(..), PrimaryKey(..), SecondaryKey(..),
                    DBLogEntry(..), Operation(..), Source(..), MerchantID(..))
import qualified EulerHS.Language as L
import           EulerHS.Types (ApiTag(..))
-- import           Servant (err500)
import           Sequelize (fromColumnar', columnize, Model, Where, Clause(..), Term(..), Set(..), modelTableName)
import           System.Random (randomRIO)
import           Unsafe.Coerce (unsafeCoerce)
import Data.Time.LocalTime (addLocalTime, LocalTime)
import Data.Time.Clock (secondsToNominalDiffTime)
import           Juspay.Extra.Config (lookupEnvT)
import qualified Data.Fixed as Fixed
import qualified Data.Serialize as Serialize
import qualified Data.Serialize as Cereal
import           Data.Either.Extra (mapRight, mapLeft)
import  EulerHS.KVConnector.Encoding ()
import           Safe (atMay)
import qualified EulerHS.Logger.Types as Log
import           Sequelize.SQLObject (ToSQLObject (..))
import           EulerHS.KVDB.Types (KVDBReply)
import qualified Database.Redis as DR
import qualified Data.Maybe as DM


jsonKeyValueUpdates ::
  forall be table. (HasCallStack, Model be table, MeshMeta be table)
  => DBCommandVersion -> [Set be table] -> [(Text, A.Value)]
jsonKeyValueUpdates version = fmap (jsonSet version)

jsonSet ::
  forall be table.
  (HasCallStack, Model be table, MeshMeta be table) =>
  DBCommandVersion -> Set be table -> (Text, A.Value)
jsonSet V1 (Set column value) = (key, modifiedValue)
  where
    key = B._fieldName . fromColumnar' . column . columnize $
      B.dbTableSettings (meshModelTableEntityDescriptor @table @be)
    modifiedValue = A.toJSON value
jsonSet V2 (Set column value) = (key, modifiedValue)
  where
    key = B._fieldName . fromColumnar' . column . columnize $
      B.dbTableSettings (meshModelTableEntityDescriptor @table @be)
    modifiedValue = A.toJSON . convertToSQLObject $ value
jsonSet _ (SetDefault _) = error "Default values are not supported"

-- | Update the model by setting it's fields according the given
--   key value mapping.
updateModel :: forall be table.
  ( MeshMeta be table,
    ToJSON (table Identity)
  ) =>
  table Identity -> [(Text, A.Value)] -> MeshResult A.Value
updateModel model updVals = do
  let updVals' = map (\(key,v) -> (AKey.fromText key, SMap.findWithDefault id key (valueMapper @be @table) v)) updVals
  case A.toJSON model of
    A.Object o -> Right (A.Object $ foldr (uncurry AKM.insert) o updVals')
    o -> Left $ MUpdateFailed
      ("Failed to update a model. Expected a JSON object but got '" <>
        (decodeUtf8 . BSL.toStrict . encodePretty $ o) <>
        "'.")

getDataFromRedisForPKey ::forall table m. (
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m) => MeshConfig -> Text -> m (MeshResult (Maybe (Text, Bool, table Identity)))
getDataFromRedisForPKey meshCfg pKey = do
  res <- L.runKVDB meshCfg.kvRedis $ L.get (fromString $ T.unpack $ pKey)
  case res of
    Right (Just r) ->
      let
        (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
      in case decodeResult  of
        Right [decodeRes] -> return . Right . Just $ (pKey, isLive, decodeRes)
        Right _ -> return . Right $ Nothing   -- Something went wrong
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
  in 
    SMap.elems result
  where
    insertKeyIntoMap key acc =
      let slot = L.keyToSlot key  -- Assuming L contains the keyToSlot function
          slotMap'' = if keyExists slot acc
                        then insertedMap slot (key : valuesForKey slot acc) acc
                        else insertedMap slot [key] acc
      in slotMap''
      where
        keyExists = SMap.member
        valuesForKey slot acc' = DM.fromJust $ SMap.lookup slot acc' 
        insertedMap  = SMap.insert 


getDataFromPKeysRedisHelper :: forall table m. (
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m) => Either KVDBReply [(Maybe ByteString)] -> m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedisHelper (Right []) = pure $ Right ([], [])
getDataFromPKeysRedisHelper (Right (Nothing : xs)) = getDataFromPKeysRedisHelper (Right xs)
getDataFromPKeysRedisHelper (Right (Just r : xs)) = do
  let (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
  case decodeResult of
    Right decodeRes -> do
      remainingPKeysResult <- getDataFromPKeysRedisHelper (Right xs)
      case remainingPKeysResult of
        Right remainingResult -> do
          if isLive
            then return $ Right (decodeRes ++ (fst remainingResult), snd remainingResult)
            else return $ Right (fst remainingResult, decodeRes ++ (snd remainingResult))
        Left err -> return $ Left err
    Left e -> return $ Left e
getDataFromPKeysRedisHelper (Left e) = return $ Left $ MRedisError e


getDataFromPKeysHelper :: forall table m. (
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m) => MeshConfig -> [[ByteString]] -> m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysHelper _ [] = pure $ Right ([], [])
getDataFromPKeysHelper meshCfg (pKey : pKeys) = do
  res <- L.runKVDB meshCfg.kvRedis $ L.mget (fromString . T.unpack . decodeUtf8 <$> pKey)
  result <- getDataFromPKeysRedisHelper res
  case result of
    Left e -> return $ Left e
    Right (a, b) -> do
      remainingPKeysResult <- getDataFromPKeysHelper meshCfg pKeys
      case remainingPKeysResult of
        Right remainingResult -> do
          return $ Right (a ++ fst remainingResult, b ++ snd remainingResult)
        Left err -> return $ Left err

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

    processResults [] = pure $ Right ([], [])
    processResults (res : rest) = case res of
      Left e -> pure $ Left (RedisPipelineError (show e))
      Right redisRes -> do
        result <- getDataFromPKeysRedisHelper redisRes
        case result of
          Left e -> pure $ Left e
          Right (a, b) -> do
            remainingResult <- processResults rest
            case remainingResult of
              Left e -> pure $ Left e
              Right (ra, rb) -> pure $ Right (a ++ ra, b ++ rb)

getDataFromPKeysRedis' :: forall table m. (
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m) => MeshConfig -> [ByteString] -> m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedis' _  []  = pure $ Right ([], [])
getDataFromPKeysRedis' meshCfg pKeys = do
  let groupedKeys = groupKeysBySlot pKeys
      (startShard, endShard) = meshCfg.tableShardModRange
  if abs (endShard - startShard ) <= 20 -- to avoid the parallelism overhead
    then getDataFromPKeysHelperAsync meshCfg groupedKeys
    else getDataFromPKeysHelper meshCfg groupedKeys 

getDataFromPKeysRedis :: forall table m. (
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m) => MeshConfig -> [ByteString] -> m (MeshResult ([table Identity], [table Identity]))
getDataFromPKeysRedis _  [] = pure $ Right ([], [])
getDataFromPKeysRedis meshCfg (pKey : pKeys)  = do
  res <- L.runKVDB meshCfg.kvRedis $ L.get (fromString $ T.unpack $ decodeUtf8 pKey)
  case res of
    Right (Just r) -> do
      let (decodeResult, isLive) = decodeToField $ BSL.fromChunks [r]
      case decodeResult of
        Right decodeRes -> do
          remainingPKeysResult <- getDataFromPKeysRedis meshCfg pKeys
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
    Right Nothing -> getDataFromPKeysRedis meshCfg pKeys
    Left e -> return $ Left $ RedisError $ (show e <> " for key: " <> show (pKey : pKeys))

------------- KEY UTILS ------------------

keyDelim:: Text
keyDelim = "_"

getPKeyWithShard :: forall table. (KVConnector (table Identity)) => table Identity -> Text -> (Int,Int) -> Text
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
  let list = map (\x -> redisKeyPrefix <> tName <> keyDelim <> getSortedKey x ) tupList
  list

secondaryKeysFiltered :: forall table. (KVConnector (table Identity)) => table Identity -> [SecondaryKey]
secondaryKeysFiltered table = filter filterEmptyValues (secondaryKeys table)
  where
    filterEmptyValues :: SecondaryKey -> Bool
    filterEmptyValues (SKey sKeyPairs) = not $ any (\p -> snd p == "") sKeyPairs

applyFPair :: (t -> b) -> (t, t) -> (b, b)
applyFPair f (x, y) = (f x, f y)

getPKeyAndValueList :: forall table. (HasCallStack, KVConnector (table Identity), A.ToJSON (table Identity)) =>  table Identity -> [(Text, A.Value)]
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
      Nothing  -> error $ "Cannot find " <> (fst x) <> " field in the row"

getSortedKey :: [(Text,Text)] -> Text
getSortedKey kvTup = do
  let sortArr = sortBy (compare `on` fst) kvTup
  let (appendedKeys, appendedValues) = applyFPair (T.intercalate "_") $ unzip sortArr
  appendedKeys <> "_" <> appendedValues

getShardedHashTag :: (Int,Int) -> Text -> Text
getShardedHashTag (start,modVal) key = do
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
    Left e    -> return $ Left $ MRedisError e

unsafeJSONSetAutoIncId :: forall table m. (ToJSON (table Identity), FromJSON (table Identity), KVConnector (table Identity), L.MonadFlow m) =>
  MeshConfig -> table Identity -> m (MeshResult (table Identity))
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
                    Left e  -> pure $ Left $ MDecodingError (show e)
                Left err -> pure $ Left err
        _ -> pure $ Left $ MDecodingError "Can't set AutoIncId value of JSON which isn't a object."
    _ -> pure $ Right obj

foldEither :: [Either a b] -> Either a [b]
foldEither [] = Right []
foldEither ((Left a) : _) = Left a
foldEither ((Right b) : xs) = mapRight ((:) b) (foldEither xs)

resultToEither :: A.Result a -> Either Text a
resultToEither (A.Success res) = Right res
resultToEither (A.Error e)     = Left $ T.pack e

mergeKVAndDBResults :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
mergeKVAndDBResults redisKeyPrefix dbRows kvRows = do
  let kvPkeys = map (getLookupKeyByPKey redisKeyPrefix) kvRows
      uniqueDbRes = filter (\r -> (getLookupKeyByPKey redisKeyPrefix r) `notElem` kvPkeys) dbRows
  kvRows ++ uniqueDbRes

getUniqueDBRes :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
getUniqueDBRes redisKeyPrefix dbRows kvRows = do
  let kvPkeys = map (getLookupKeyByPKey redisKeyPrefix) kvRows
  filter (\r -> (getLookupKeyByPKey redisKeyPrefix r) `notElem` kvPkeys) dbRows

removeDeleteResults :: KVConnector (table Identity) => Text -> [table Identity] -> [table Identity] -> [table Identity]
removeDeleteResults redisKeyPrefix delRows rows = do
  let delPKeys = map (getLookupKeyByPKey redisKeyPrefix) delRows
      nonDelRows = filter (\r -> (getLookupKeyByPKey redisKeyPrefix r) `notElem` delPKeys) rows
  nonDelRows

getLatencyInMicroSeconds :: Integer -> Integer
getLatencyInMicroSeconds execTime = execTime `div` 1000000

---------------- Match where clauses -------------
findOneMatching :: B.Beamable table => Where be table -> [table Identity] -> Maybe (table Identity)
findOneMatching whereClause = find (`matchWhereClause` whereClause)

findAllMatching :: B.Beamable table => Where be table -> [table Identity] -> [table Identity]
findAllMatching whereClause = filter (`matchWhereClause` whereClause)

matchWhereClause :: B.Beamable table => table Identity -> [Clause be table] -> Bool
matchWhereClause row = all matchClauseQuery
  where
  matchClauseQuery = \case
    And queries     -> all matchClauseQuery queries
    Or queries      -> any matchClauseQuery queries
    Is column' term ->
      let column = fromColumnar' . column' . columnize
        in termQueryMatch (column row) term

termQueryMatch :: (Ord value, ToJSON value) => value -> Term be value -> Bool
termQueryMatch columnVal = \case
  In literals             -> any (matchWithCaseInsensitive columnVal) literals
  Null                    -> isNothing columnVal
  Eq literal              -> matchWithCaseInsensitive columnVal literal
  GreaterThan literal     -> columnVal > literal
  GreaterThanOrEq literal -> columnVal >= literal
  LessThan literal        -> columnVal < literal
  LessThanOrEq literal    -> columnVal <= literal
  Not Null                -> isJust columnVal
  Not (Eq literal)        -> not $ matchWithCaseInsensitive columnVal literal
  Not term                -> not (termQueryMatch columnVal term)
  _                       -> error "Term query not supported"

  where
    matchWithCaseInsensitive c1 c2 =
      if c1 == c2
        then True
        else -- Fallback to case insensitive check (DB supports this)
          case (toJSON c1, toJSON c2) of
            (A.String s1, A.String s2) -> T.toLower s1 == T.toLower s2
            _ -> c1 == c2

-- || Helper function to filter out rows based on where clause
-- || Do not use this as this is not efficient 
getFilteredWhereClause :: forall be table. (B.Beamable table) => [table Identity] -> [Clause be table] -> [Clause be table]
getFilteredWhereClause rows = map matchClauseQuery
  where
    matchClauseQuery :: Clause be table -> Clause be table
    matchClauseQuery = \case
      And queries -> And $ map matchClauseQuery queries
      Or queries  -> Or $ map matchClauseQuery queries
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
getConfigStreamNames = fmap (\shardNo -> getStreamName (show shardNo) ) [1..getConfigStreamMaxShards]

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
    let
      jitterInSec = getConfigEntryTtlJitterInSeconds
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
    in  case decodeRes of
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
                                    Left e   -> Left $ MDecodingError $ T.pack e
                                _      ->
                                  case A.eitherDecode val of
                                    Right r' -> decodeField @a r'
                                    Left e   -> Left $ MDecodingError $ T.pack e
              in (decodedMeshResult, isLive)

decodeField :: forall a. (FromJSON a, Serialize.Serialize a) => A.Value -> MeshResult [a]
decodeField o@(A.Object _) =
  case A.eitherDecode @a $ A.encode o of
    Right r -> return [r]
    Left e  -> Left $ MDecodingError $ T.pack e
decodeField o@(A.Array _) =
  mapLeft (MDecodingError . T.pack)
    $ A.eitherDecode @[a] $ A.encode o
decodeField o = Left $ MDecodingError
  ("Expected list or object but got '" <> T.pack (show o) <> "'.")

getFieldsAndValuesFromClause :: forall table be. (Model be table, MeshMeta be table) =>
  B.DatabaseEntityDescriptor be (B.TableEntity table) -> Clause be table -> [[(Text, Text)]]
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
    processAnd xs ys = [x ++ y | x <-xs, y <- ys]
    processOr xs = concatMap (getFieldsAndValuesFromClause dt) xs

    showVal res = case res of
      A.String r -> r
      A.Number n -> T.pack $ show n
      A.Array l  -> T.pack $ show l
      A.Object o -> T.pack $ show o
      A.Bool b -> T.pack $ show b
      A.Null -> T.pack ""

getPrimaryKeyFromFieldsAndValues :: (L.MonadFlow m) => Text -> MeshConfig -> HM.HashMap Text Bool -> [(Text, Text)] -> m (MeshResult [ByteString])
getPrimaryKeyFromFieldsAndValues _ _ _ [] = pure $ Right []
getPrimaryKeyFromFieldsAndValues modelName meshCfg keyHashMap fieldsAndValues = do
  res <- foldEither <$> mapM getPrimaryKeyFromFieldAndValueHelper fieldsAndValues
  pure $ mapRight (intersectList . catMaybes) res
  where

    getPrimaryKeyFromFieldAndValueHelper (k, v) = do
      let constructedKey = meshCfg.redisKeyPrefix <> modelName <> "_" <> k <> "_" <> v
      case HM.lookup k keyHashMap of
        Just True -> pure $ Right $ Just [fromString $ T.unpack (constructedKey <> getShardedHashTag meshCfg.tableShardModRange constructedKey)]
        Just False -> do
          res <- L.runKVDB meshCfg.kvRedis $ L.smembers (fromString $ T.unpack constructedKey)
          case res of
            Right r -> pure $ Right $ Just r
            Left e -> pure $ Left $ RedisError $ (show e <> " for key: " <> show constructedKey)
        _ -> pure $ Right Nothing

    intersectList (x : y : xs) = intersectList (intersect x y : xs)
    intersectList (x : [])     = x
    intersectList []           = []

filterPrimaryAndSecondaryKeys :: HM.HashMap Text Bool -> [(Text, Text)] -> [(Text, Text)]
filterPrimaryAndSecondaryKeys keyHashMap fieldsAndValues = filter (\(k, _) -> HM.member k keyHashMap) fieldsAndValues

getSecondaryKeyLength :: HM.HashMap Text Bool -> [(Text, Text)] -> Int
getSecondaryKeyLength keyHashMap = length . filter (\(k, _) -> HM.lookup k keyHashMap == Just False)

mkUniq :: Ord a => [a] -> [a] -- O(n log n)
mkUniq = Set.toList . Set.fromList

-- >>> map (T.intercalate "_") (nonEmptySubsequences ["id", "id2", "id3"])
-- ["id","id2","id_id2","id3","id_id3","id2_id3","id_id2_id3"]
nonEmptySubsequences         :: [Text] -> [[Text]]
nonEmptySubsequences []      =  []
nonEmptySubsequences (x:xs)  =  [x]: foldr f [] (nonEmptySubsequences xs)
  where f ys r = ys : (x : ys) : r

whereClauseDiffCheck :: forall be table m.
  ( L.MonadFlow m
  , Model be table
  , MeshMeta be table
  , KVConnector (table Identity)
  ) =>
  Where be table -> m (Maybe [[Text]])
whereClauseDiffCheck whereClause =
  if isWhereClauseDiffCheckEnabled then do
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

shouldLogFindDBCallLogs :: Bool
shouldLogFindDBCallLogs = fromMaybe False $ readMaybe =<< lookupEnvT  @String "IS_FIND_DB_LOGS_ENABLED"

redisCallsHardLimit :: Int
redisCallsHardLimit = fromMaybe 5000 $ readMaybe =<< lookupEnvT @String "REDIS_CALLS_HARD_LIMIT"

redisCallsSoftLimit :: Int
redisCallsSoftLimit = fromMaybe 500 $ readMaybe =<< lookupEnvT @String "REDIS_CALLS_SOFT_LIMIT"

lengthOfLists :: [[a]] -> Int
lengthOfLists = foldl' (\acc el -> acc + length el) 0

isLogsEnabledForModel :: Text -> Bool
isLogsEnabledForModel modelName = do
  let env :: Text = fromMaybe "development" $ lookupEnvT "NODE_ENV"
  if env == "production" then do
    let enableModelList = fromMaybe [] $ readMaybe =<< lookupEnvT @String "IS_LOGS_ENABLED_FOR_MODEL"
    modelName `elem` enableModelList
    else True

logAndIncrementKVMetric :: (L.MonadFlow m, ToJSON a) => Bool -> Text -> Operation -> MeshResult a -> Int -> Text -> Integer -> Source -> Maybe [[Text]] -> m ()
logAndIncrementKVMetric shouldLogData action operation res latency model cpuLatency source mbDiffCheckRes = do
  apiTag <- L.getOptionLocal ApiTag
  mid    <- L.getOptionLocal MerchantID
  let shouldLogData_  = isLogsEnabledForModel model && shouldLogData
  let dblog = DBLogEntry {
      _log_type     = "DB"
    , _action       = action -- For logprocessor
    , _operation    = operation
    , _data         = case res of
                        Left err -> A.String (T.pack $ show err)
                        Right m  -> if shouldLogData_ then A.toJSON m else A.Null
    , _latency      = latency
    , _model        = model
    , _cpuLatency   = getLatencyInMicroSeconds cpuLatency
    , _source       = source
    , _apiTag       = apiTag
    , _merchant_id  = mid
    , _whereDiffCheckRes = mbDiffCheckRes
    }
  if action == "FIND" then
    when shouldLogFindDBCallLogs $ logDb Log.Debug ("DB" :: Text) source action model latency dblog
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
