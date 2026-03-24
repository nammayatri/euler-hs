{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# LANGUAGE BangPatterns #-}

module EulerHS.KVConnector.Flow
  (
    createWoReturingKVConnector,
    createWithKVConnector,
    findWithKVConnector,
    updateWoReturningWithKVConnector,
    updateWithKVConnector,
    findAllWithKVConnector,
    updateAllWithKVConnector,
    getFieldsAndValuesFromClause,
    updateAllReturningWithKVConnector,
    findAllWithOptionsKVConnector,
    findAllWithOptionsKVConnector',
    deleteWithKVConnector,
    deleteReturningWithKVConnector,
    deleteAllReturningWithKVConnector,
    findAllWithKVAndConditionalDBInternal,
    findOneFromKvRedis,
    findAllFromKvRedis
  )
 where

import           EulerHS.Prelude hiding (maximum)
import EulerHS.CachedSqlDBQuery
    ( runQuery,
      findAllSql,
      createReturning,
      createSqlWoReturing,
      updateOneSqlWoReturning,
      SqlReturning(..) )
import           EulerHS.KVConnector.Types (DBCommandVersion (..), KVConnector(..), MeshConfig(..), MeshResult, MeshError(..), MeshMeta(..), SecondaryKey(..), tableName, keyMap, Source(..), TableMappings(..))
import           EulerHS.KVConnector.DBSync (getCreateQuery, getUpdateQuery, getDeleteQuery, getDbDeleteCommandJson, getDbUpdateCommandJson, getDbUpdateCommandJsonWithPrimaryKey, getDbDeleteCommandJsonWithPrimaryKey,getCreateQueryWithCompression, getDeleteQueryWithCompression, getUpdateQueryWithCompression)
import           EulerHS.KVConnector.InMemConfig.Flow (searchInMemoryCache, pushToInMemConfigStream, fetchRowFromDBAndAlterImc)
import           EulerHS.KVConnector.InMemConfig.Types (ImcStreamCommand(..))
import           EulerHS.KVConnector.Utils
import           EulerHS.KVDB.Types (KVDBReply)
import           Control.Arrow ((>>>))
import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BSL
import           Data.List (maximum)
import qualified Data.Text as T
import qualified EulerHS.Language as L
import qualified Data.HashMap.Strict as HM
import           EulerHS.SqlDB.Types (BeamRunner, BeamRuntime, DBConfig, DBError(..))
import qualified EulerHS.SqlDB.Language as DB
import           Sequelize (fromColumnar', columnize, sqlSelect, sqlSelect', sqlUpdate, sqlDelete, Model, Where, Clause(..), Set(..), OrderBy(..))
import qualified Database.Beam as B
import qualified Database.Beam.Postgres as BP
import           Data.Either.Extra (mapRight, mapLeft)
import           Named (defaults, (!))
import qualified Data.Serialize as Serialize
import qualified EulerHS.KVConnector.Encoding as Encoding
import qualified Data.Maybe as DMaybe
import qualified EulerHS.KVConnector.Metrics as Metrics
import           EulerHS.KVConnector.Compression
import           EulerHS.KVConnector.Helper.Utils

createWoReturingKVConnector :: forall (table :: (Type -> Type) -> Type) be m beM.
  ( HasCallStack,
    SqlReturning beM be,
    Model be table,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    FromJSON (table Identity),
    TableMappings(table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    KVConnector (table Identity),
    L.MonadFlow m) =>
  DBConfig beM ->
  MeshConfig ->
  table Identity ->
  m (MeshResult ())
createWoReturingKVConnector dbConf meshCfg value = do
  let isEnabled = meshCfg.meshEnabled && not meshCfg.kvHardKilled
  if isEnabled
    then do
      mapRight (const ()) <$> createKV meshCfg value
    else do
      res <- createSqlWoReturing dbConf value
      case res of
        Right _ -> return $ Right ()
        Left e -> return $ Left $ MDBError e


createWithKVConnector ::
  forall (table :: (Type -> Type) -> Type) be m beM.
  ( HasCallStack,
    SqlReturning beM be,
    Model be table,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    TableMappings (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    KVConnector (table Identity),
    L.MonadFlow m) =>
  DBConfig beM ->
  MeshConfig ->
  table Identity ->
  m (MeshResult (table Identity))
createWithKVConnector dbConf meshCfg value = do
  let isEnabled = meshCfg.meshEnabled && not meshCfg.kvHardKilled
  res <- if isEnabled
    then do
      createKV meshCfg value
    else do
      res <- createReturning dbConf value Nothing
      case res of
        Right val -> return $ Right val
        Left e -> return $ Left $ MDBError e
  when meshCfg.memcacheEnabled $ do
    case res of
      Right obj -> pushToInMemConfigStream meshCfg ImcInsert obj
      Left _    -> pure ()
  pure res

createKV :: forall (table :: (Type -> Type) -> Type) m.
  ( FromJSON (table Identity),
    ToJSON (table Identity),
    TableMappings (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    KVConnector (table Identity),
    L.MonadFlow m) =>
  MeshConfig ->
  table Identity ->
  m (MeshResult (table Identity))
createKV meshCfg value = do
  autoIncIdRes <- unsafeJSONSetAutoIncId meshCfg value
  case autoIncIdRes of
    Right val -> do
      let pKeyText = getLookupKeyByPKey meshCfg.redisKeyPrefix val
          shard = getShardedHashTag meshCfg.tableShardModRange pKeyText
          pKey = fromString . T.unpack $ pKeyText <> shard
      time <- fromIntegral <$> L.getCurrentDateInMillis

      qCmd <- if isCompressionAllowed
        then do getCreateQueryWithCompression (getTableName @(table Identity)) (pKeyText <> shard) time meshCfg.meshDBName val (getTableMappings @(table Identity)) meshCfg.forceDrainToDB
        else do pure $ BSL.toStrict $ A.encode $ getCreateQuery (getTableName @(table Identity)) (pKeyText <> shard) time meshCfg.meshDBName val (getTableMappings @(table Identity)) meshCfg.forceDrainToDB

      revMappingRes <- mapM (\secIdx -> do
        let sKey = fromString . T.unpack $ secIdx
        _ <- L.runKVDB meshCfg.kvRedis $ L.sadd sKey [pKey]
        L.runKVDB meshCfg.kvRedis $ L.expire sKey meshCfg.redisTtl
        ) $ getSecondaryLookupKeys meshCfg.redisKeyPrefix val
      case foldEither revMappingRes of
        Left err -> pure $ Left $ RedisError (show err <> "Primary key => " <> show pKey <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix val) <> " in createKV")
        Right _ -> do
          kvRes <- L.runKVDB meshCfg.kvRedis $ L.multiExecWithHash (encodeUtf8 shard) $ do
            _ <- L.xaddTx
                  (encodeUtf8 (meshCfg.ecRedisDBStream <> shard))
                  L.AutoID
                  [("command", qCmd)]
            L.setexTx pKey meshCfg.redisTtl (BSL.toStrict $ Encoding.encode_ meshCfg.cerealEnabled val)
          case kvRes of
            Right _ -> pure $ Right val
            Left err -> pure $ Left $ RedisError (show err <> "Primary key => " <> show pKey <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix val) <> " in createKV")
    Left err -> pure $ Left err


createInRedis :: forall (table :: (Type -> Type) -> Type) m.
  ( FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    KVConnector (table Identity),
    L.MonadFlow m) =>
  MeshConfig ->
  table Identity ->
  m (MeshResult (table Identity))
createInRedis meshCfg val = do
  let pKeyText = getLookupKeyByPKey meshCfg.redisKeyPrefix val
      shard = getShardedHashTag meshCfg.tableShardModRange pKeyText
      pKey = fromString . T.unpack $ pKeyText <> shard
  revMappingRes <- mapM (\secIdx -> do
    let sKey = fromString . T.unpack $ secIdx
    _ <- L.runKVDB meshCfg.kvRedis $ L.sadd sKey [pKey]
    L.runKVDB meshCfg.kvRedis $ L.expire sKey meshCfg.redisTtl
    ) $ getSecondaryLookupKeys meshCfg.redisKeyPrefix val
  case foldEither revMappingRes of
    Left err -> pure $ Left $ RedisError (show err <> "Primary key => " <> show pKey <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix val))
    Right _ -> do
      kvRes <- L.runKVDB meshCfg.kvRedis $ L.multiExecWithHash (encodeUtf8 shard) $
        L.setexTx pKey meshCfg.redisTtl (BSL.toStrict $ Encoding.encode_ meshCfg.cerealEnabled val)
      case kvRes of
        Right _ -> pure $ Right val
        Left err -> pure $ Left (RedisError (show err <> "Primary key => " <> show pKey <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix val)))

---------------- Update -----------------

updateWoReturningWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    SqlReturning beM be,
    Model be table,
    MeshMeta be table,
    TableMappings (table Identity),
    B.HasQBuilder be,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity), --debugging purpose
    L.MonadFlow m) =>
  DBConfig beM ->
  DBConfig beM ->
  MeshConfig ->
  [Set be table] ->
  Where be table ->
  m (MeshResult ())
updateWoReturningWithKVConnector dbConf replicaDbConfig meshCfg setClause whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  (_, res) <- if not isDisabled
    then do
      -- Discarding object
      (\updRes -> (fst updRes, mapRight (const ()) (snd updRes))) <$> modifyOneKV dbConf replicaDbConfig meshCfg whereClause (Just setClause) True True
    else do
      res <- updateOneSqlWoReturning dbConf setClause whereClause
      (SQL,) <$> case res of
        Right val -> do
           {-
              Since beam-mysql doesn't implement updateRowsReturning, we fetch the row from imc (or lower layers)
              and then update the json so fetched and finally setting it in the imc.
            -}
            if meshCfg.memcacheEnabled
              then fetchRowFromDBAndAlterImc replicaDbConfig meshCfg whereClause ImcInsert
              else return $ Right val

        Left e -> return $ Left $ MDBError e
  pure res


updateWithKVConnector :: forall table m.
  ( HasCallStack,
    Model BP.Postgres table,
    MeshMeta BP.Postgres table,
    B.HasQBuilder BP.Postgres,
    TableMappings (table Identity),
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  DBConfig BP.Pg ->
  DBConfig BP.Pg ->
  MeshConfig ->
  [Set BP.Postgres table] ->
  Where BP.Postgres table ->
  m (MeshResult (Maybe (table Identity)))
updateWithKVConnector dbConf replicaDbConfig meshCfg setClause whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  (_, res) <- if not isDisabled
    then do
      modifyOneKV dbConf replicaDbConfig meshCfg whereClause (Just setClause) False True
    else do
      let updateQuery = DB.updateRowsReturningList $ sqlUpdate ! #set setClause ! #where_ whereClause
      res <- runQuery dbConf updateQuery
      (SQL, ) <$> case res of
        Right [x] -> do
          when meshCfg.memcacheEnabled $ pushToInMemConfigStream meshCfg ImcInsert x
          return $ Right (Just x)
        Right [] -> return $ Right Nothing
        Right xs -> do
          let message = "DB returned \"" <> show (length xs) <> "\" rows after update for table: " <> show (tableName @(table Identity))
          L.logError @Text "updateWithKVConnector" message
          return $ Left $ UnexpectedError message
        Left e -> return $ Left $ MDBError e
  pure res

modifyOneKV :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    B.HasQBuilder be,
    KVConnector (table Identity),
    TableMappings (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    Show (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  Maybe [Set be table] ->
  Bool ->
  Bool ->
  m (Source, MeshResult (Maybe (table Identity)))
modifyOneKV dbConf replicaDbConfig meshCfg whereClause mbSetClause updateWoReturning isLive = do
  let setClause = fromMaybe [] mbSetClause
      updVals = jsonKeyValueUpdates V1 setClause  
  -- First check primary Redis only (with secondaryRedisEnabled disabled)
  let primaryOnlyCfg = meshCfg { secondaryRedisEnabled = False }
  primaryKvResult <- findOneFromRedis primaryOnlyCfg whereClause
  case primaryKvResult of
    Right ([], []) -> do
      -- Not found in primary Redis, check secondary Redis if enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Check secondary Redis by treating it as primary (with secondaryRedisEnabled disabled)
          let secondaryAsPrimaryCfg = meshCfg { kvRedis = meshCfg.kvRedisSecondary, secondaryRedisEnabled = False }
          secondaryKvResult <- findOneFromRedis secondaryAsPrimaryCfg whereClause
          processKvResult secondaryKvResult meshCfg.kvRedisSecondary updVals setClause
        else updateInKVOrSQL Nothing updVals setClause meshCfg.kvRedis
    Right ([], _) -> do
      L.logDebugT "modifyOneKV" ("Modifying nothing - Row is deleted already for " <> tableName @(table Identity))
      pure (KV, Right Nothing)
    Right (_, _) -> processKvResult primaryKvResult meshCfg.kvRedis updVals setClause
    Left err -> pure (KV, Left err)
    
  where
    processKvResult :: MeshResult ([table Identity], [table Identity]) -> Text -> [(Text, A.Value)] -> [Set be table] -> m (Source, MeshResult (Maybe (table Identity)))
    processKvResult kvResult redisConn updVals setClause = case kvResult of
      Right ([], []) -> updateInKVOrSQL Nothing updVals setClause redisConn
      Right ([], _) -> do
        L.logDebugT "modifyOneKV" ("Modifying nothing - Row is deleted already for " <> tableName @(table Identity))
        pure (KV, Right Nothing)
      Right (kvLiveRows, _) -> do
        findFromDBIfMatchingFailsRes <- findFromDBIfMatchingFails meshCfg replicaDbConfig whereClause kvLiveRows
        case findFromDBIfMatchingFailsRes of
          (_, Right [])        -> pure (KV, Right Nothing)
          (SQL, Right [dbRow]) -> updateInKVOrSQL (Just dbRow) updVals setClause redisConn
          (KV, Right [obj])   -> (KV,) . mapRight Just <$> (if isLive
             then updateObjectRedis meshCfg redisConn updVals setClause False whereClause obj
             else deleteObjectRedis meshCfg redisConn False whereClause obj)
          (source, Right _)   -> do
            L.logErrorT "modifyOneKV" ("Found more than one record in redis - Modification failed (redis: " <> redisConn <> ")")
            pure (source, Left $ MUpdateFailed "Found more than one record in redis")
          (source, Left err) -> pure (source, Left err)
      Left err -> pure (KV, Left err)

    alterImc :: Maybe (table Identity) -> m (MeshResult ())
    alterImc mbRow = do
      case (isLive, mbRow) of
        (True, Nothing) -> fetchRowFromDBAndAlterImc replicaDbConfig meshCfg whereClause ImcInsert
        (True, Just x) -> Right <$> pushToInMemConfigStream meshCfg ImcInsert x
        (False, Nothing) ->
            searchInMemoryCache meshCfg dbConf whereClause >>= (snd >>> \case
              Left e -> return $ Left e
              Right rs -> Right <$> mapM_ (pushToInMemConfigStream meshCfg ImcDelete) rs)
        (False, Just x) -> Right <$> pushToInMemConfigStream meshCfg ImcDelete x

    runUpdateOrDelete setClause = do
      case (isLive, updateWoReturning) of
          (True, True) -> do
            res <- updateOneSqlWoReturning dbConf setClause whereClause
            case res of
              Right _ -> pure $ Right Nothing
              Left e -> return $ Left $ MDBError e
          (True, False) -> do
            let updateQuery = DB.updateRowsReturningList $ sqlUpdate ! #set setClause ! #where_ whereClause
            res <- runQuery dbConf updateQuery
            case res of
              Right [x] -> return $ Right (Just x)
              Right [] -> return $ Right Nothing
              Right xs -> do
                let message = "DB returned " <> show (length xs) <> " rows after update for table: " <> show (tableName @(table Identity))
                L.logErrorT "updateWithKVConnector" message
                return $ Left $ UnexpectedError message
              Left e -> return $ Left $ MDBError e
          (False, True) -> do
            let deleteQuery = DB.deleteRows $ sqlDelete ! #where_ whereClause
            res <- runQuery dbConf deleteQuery
            case res of
                Right _ -> return $ Right Nothing
                Left e  -> return $ Left $ MDBError e
          (False, False) -> do
            res <- deleteAllReturning dbConf whereClause
            case res of
                Right [x] -> return $ Right (Just x)
                Right [] -> return $ Right Nothing
                Right xs -> do
                  let message = "DB returned " <> show (length xs) <> " rows after delete for table: " <> show (tableName @(table Identity))
                  L.logErrorT "deleteReturningWithKVConnector" message
                  return $ Left $ UnexpectedError message
                Left e -> return $ Left $ MDBError e

    updateInKVOrSQL maybeRow updVals setClause redisConn = do
        if isRecachingEnabled && meshCfg.meshEnabled
          then do
            dbRes <- case maybeRow of
              Nothing -> findOneFromDB replicaDbConfig whereClause
              Just dbrow -> pure $ Right $ Just dbrow
            (KV,) <$> case dbRes of
              Right (Just obj) -> do
                reCacheDBRowsRes <- reCacheDBRows meshCfg [obj]
                case reCacheDBRowsRes of
                  Left err -> return $ Left $ RedisError (show err <> "Primary key => " <> show (getLookupKeyByPKey meshCfg.redisKeyPrefix obj) <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix obj) <> " in updateInKVOrSQL")
                  Right _  -> mapRight Just <$> if isLive
                    then updateObjectRedis meshCfg redisConn updVals setClause False whereClause obj
                    else deleteObjectRedis meshCfg redisConn False whereClause obj
              Right Nothing -> pure $ Right Nothing
              Left err -> pure $ Left err
          else (SQL,) <$> (do
            runUpdateOrDelete setClause >>= \case
              Left e -> return $ Left e
              Right mbRow -> if meshCfg.memcacheEnabled
                  then
                    alterImc mbRow <&> ($> mbRow)
                  else
                    return . Right $ mbRow
            )

updateObjectInMemConfig :: forall be table m.
  ( HasCallStack,
    MeshMeta be table ,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    L.MonadFlow m
  ) => MeshConfig -> Where be table -> [(Text, A.Value)] -> table Identity ->  m (MeshResult ())
updateObjectInMemConfig meshCfg _ updVals obj = do
  let shouldUpdateIMC = meshCfg.memcacheEnabled
  if not shouldUpdateIMC
    then pure . Right $ ()
    else
      case (updateModel @be @table) obj updVals of
        Left err -> return $ Left err
        Right updatedModelJson ->
          case A.fromJSON updatedModelJson of
            A.Error decodeErr -> return . Left . MDecodingError . T.pack $ decodeErr
            A.Success (updatedModel' :: table Identity)  -> do
              pushToInMemConfigStream meshCfg ImcInsert updatedModel'
              pure . Right $ ()



updateObjectRedis :: forall beM be table m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    Model be table,
    MeshMeta be table,
    TableMappings (table Identity),
    B.HasQBuilder be,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    -- Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  MeshConfig -> Text -> [(Text, A.Value)] -> [Set be table] -> Bool -> Where be table -> table Identity -> m (MeshResult (table Identity))
updateObjectRedis meshCfg redisConn updVals setClauses addPrimaryKeyToWhereClause whereClause obj = do
  let modelName = tableName @(table Identity)
      clusterName = if redisConn == meshCfg.kvRedisSecondary then "secondaryCluster" else "primaryCluster"
  
  Metrics.withKVLatencyMetric "REDIS_UPDATE" modelName clusterName $ do
    configUpdateResult <- updateObjectInMemConfig meshCfg whereClause updVals obj
    when (isLeft configUpdateResult) $ L.logErrorT "MEMCONFIG_UPDATE_ERROR" (show configUpdateResult)
    case (updateModel @be @table) obj updVals of
      Left err -> return $ Left err
      Right updatedModel -> do
        time <- fromIntegral <$> L.getCurrentDateInMillis
        let pKeyText  = getLookupKeyByPKey meshCfg.redisKeyPrefix obj
            shard     = getShardedHashTag meshCfg.tableShardModRange pKeyText
            pKey      = fromString . T.unpack $ pKeyText <> shard
            updateCmd = if addPrimaryKeyToWhereClause
                          then getDbUpdateCommandJsonWithPrimaryKey (getTableName @(table Identity)) setClauses obj whereClause
                          else getDbUpdateCommandJson (getTableName @(table Identity)) setClauses whereClause

        qCmd <- if isCompressionAllowed
          then do getUpdateQueryWithCompression (pKeyText <> shard) time meshCfg.meshDBName updateCmd (getTableMappings @(table Identity)) updatedModel (getTableName @(table Identity)) meshCfg.forceDrainToDB
          else do pure $ BSL.toStrict $ A.encode $ getUpdateQuery (pKeyText <> shard) time meshCfg.meshDBName updateCmd (getTableMappings @(table Identity)) updatedModel meshCfg.forceDrainToDB
 
        case resultToEither $ A.fromJSON updatedModel of
          Right value -> do
            let olderSkeys = map (\(SKey s) -> s) (secondaryKeysFiltered obj)
            skeysUpdationRes <- modifySKeysRedis olderSkeys value
            case skeysUpdationRes of
              Right _ -> do
                kvdbRes <- L.runKVDB redisConn $ L.multiExecWithHash (encodeUtf8 shard) $ do
                  _ <- L.xaddTx
                        (encodeUtf8 (meshCfg.ecRedisDBStream <> shard))
                        L.AutoID
                        [("command", qCmd)]
                  L.setexTx pKey meshCfg.redisTtl (BSL.toStrict $ Encoding.encode_ meshCfg.cerealEnabled value)
                
                case kvdbRes of
                  Right _ -> pure $ Right value
                  Left err -> pure $ Left $ RedisError (show err <> "Primary key => " <> show pKey <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix value) <> " in updateObjectRedis")
              Left err -> pure $ Left err
          Left err -> pure $ Left $ MDecodingError err

  where
    modifySKeysRedis :: [[(Text, Text)]] -> table Identity -> m (MeshResult (table Identity)) -- TODO: Optimise this logic
    modifySKeysRedis olderSkeys table = do
      let pKeyText = getLookupKeyByPKey meshCfg.redisKeyPrefix table
          shard = getShardedHashTag meshCfg.tableShardModRange pKeyText
          pKey = fromString . T.unpack $ pKeyText <> shard
      let tName = tableName @(table Identity)
          updValsMap = HM.fromList (map (\p -> (fst p, True)) updVals)
          (modifiedSkeysOld, unModifiedSkeys) = applyFPair (map (getSortedKeyAndValue tName)) $
                                                segregateList (`isKeyModified` updValsMap) olderSkeys
          newSkeys =  map (\(SKey s) -> s) (secondaryKeys table)
          (modifiedSkeysNew, _) = applyFPair (map (getSortedKeyAndValue tName)) $
                                                segregateList (`isKeyModified` updValsMap) newSkeys
      mapRight (const table) <$> runExceptT (do
                                    mapM_ (ExceptT . resetTTL) unModifiedSkeys
                                    mapM_ (ExceptT . deletePkeyFromSkey pKey) modifiedSkeysOld
                                    mapM_ (ExceptT . addPkeyToSkey pKey) modifiedSkeysNew)

    resetTTL Nothing = pure $ Right False
    resetTTL (Just key) = do
      x <- L.rExpire redisConn (fromString $ T.unpack key) meshCfg.redisTtl
      pure $ mapLeft MRedisError x

    deletePkeyFromSkey _ Nothing = pure $ Right 0
    deletePkeyFromSkey pKey (Just key) = mapLeft MRedisError <$> L.sRemB redisConn (fromString $ T.unpack key) [pKey]

    addPkeyToSkey :: ByteString -> Maybe Text -> m (MeshResult Bool)
    addPkeyToSkey _ Nothing = pure $ Right False
    addPkeyToSkey pKey (Just key) = do
      _ <- L.rSadd redisConn (fromString $ T.unpack key) [pKey]
      mapLeft MRedisError <$> L.rExpireB redisConn (fromString $ T.unpack key) meshCfg.redisTtl

    getSortedKeyAndValue :: Text -> [(Text,Text)] -> Maybe Text
    getSortedKeyAndValue tName kvTup = do
      let sortArr = sortBy (compare `on` fst) kvTup
      let (appendedKeys, appendedValues) = applyFPair (T.intercalate "_") $ unzip sortArr
      if any (\(_, v) -> v=="") kvTup
        then Nothing
        else Just $ meshCfg.redisKeyPrefix <> tName <> "_" <> appendedKeys <> "_" <> appendedValues

    isKeyModified :: [(Text, Text)] -> HM.HashMap Text Bool -> Bool
    isKeyModified sKey updValsMap = foldl' (\r k -> HM.member (fst k) updValsMap || r) False sKey

    segregateList :: (a -> Bool) -> [a] -> ([a], [a])
    segregateList func list = go list ([], [])
      where
        go [] res     = res
        go (x : xs) (trueList, falseList)
          | func x    = go xs (x : trueList, falseList)
          | otherwise = go xs (trueList, x : falseList)


updateAllReturningWithKVConnector :: forall table m.
  ( HasCallStack,
    Model BP.Postgres table,
    MeshMeta BP.Postgres table,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    TableMappings (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  DBConfig BP.Pg ->
  DBConfig BP.Pg ->
  MeshConfig ->
  [Set BP.Postgres table] ->
  Where BP.Postgres table ->
  m (MeshResult [table Identity])
updateAllReturningWithKVConnector dbConf replicaDbConfig meshCfg setClause whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      let updVals = jsonKeyValueUpdates V1 setClause
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis, and DB in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows, dbRows) <- callMultiAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
            (findAllSql replicaDbConfig whereClause)
          -- Process with segregated Redis results
          updateKVAndDBResultsMultiCloud meshCfg whereClause dbRows primaryKvRows secondaryKvRows (Just updVals) False dbConf (Just setClause) True
        else do
          -- Original single Redis logic
          (kvRows, dbRows) <- callKVDBAsync (redisFindAll meshCfg whereClause) (findAllSql replicaDbConfig whereClause)
          updateKVAndDBResults meshCfg whereClause dbRows kvRows (Just updVals) False dbConf (Just setClause) True
    else do
      let updateQuery = DB.updateRowsReturningList $ sqlUpdate ! #set setClause ! #where_ whereClause
      res <- runQuery dbConf updateQuery
      case res of
        Right x -> do
          when meshCfg.memcacheEnabled $
            mapM_ (pushToInMemConfigStream meshCfg ImcInsert ) x
          return $ Right x
        Left e -> return $ Left $ MDBError e

updateAllWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    BeamRunner beM,
    Model be table,
    MeshMeta be table,
    B.HasQBuilder be,
    TableMappings (table Identity),
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  DBConfig beM ->
  DBConfig beM ->
  MeshConfig ->
  [Set be table] ->
  Where be table ->
  m (MeshResult ())
updateAllWithKVConnector dbConf replicaDbConfig meshCfg setClause whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      let updVals = jsonKeyValueUpdates V1 setClause
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis, and DB in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows, dbRows) <- callMultiAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
            (findAllSql replicaDbConfig whereClause)
          -- Process with segregated Redis results
          mapRight (const ()) <$> updateKVAndDBResultsMultiCloud meshCfg whereClause dbRows primaryKvRows secondaryKvRows (Just updVals) True dbConf (Just setClause) True
        else do
          -- Original single Redis logic
          (kvRows, dbRows) <- callKVDBAsync (redisFindAll meshCfg whereClause) (findAllSql replicaDbConfig whereClause)
          mapRight (const ()) <$> updateKVAndDBResults meshCfg whereClause dbRows kvRows (Just updVals) True dbConf (Just setClause) True
    else do
      let updateQuery = DB.updateRows $ sqlUpdate ! #set setClause ! #where_ whereClause
      res <- runQuery dbConf updateQuery
      case res of
        Right _ -> do
          let findAllQuery = DB.findRows (sqlSelect ! #where_ whereClause ! defaults)
          dbRes <- runQuery replicaDbConfig findAllQuery
          case dbRes of
            Right dbRows -> do
              when meshCfg.memcacheEnabled $
                mapM_ (pushToInMemConfigStream meshCfg ImcInsert) dbRows
              return . Right $ ()
            Left e -> return . Left . MDBError $ e
        Left e -> return $ Left $ MDBError e

updateKVAndDBResults :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    BeamRunner beM,
    Model be table,
    MeshMeta be table,
    TableMappings (table Identity),
    B.HasQBuilder be,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) => MeshConfig -> Where be table -> Either DBError [table Identity] -> MeshResult ([table Identity], [table Identity]) -> Maybe [(Text, A.Value)] -> Bool -> DBConfig beM -> Maybe [Set be table] -> Bool -> m (MeshResult [table Identity])
updateKVAndDBResults meshCfg whereClause eitherDbRows eitherKvRows mbUpdateVals updateWoReturning dbConf mbSetClause isLive = do
  let setClause = fromMaybe [] mbSetClause --Change this logic
      updVals = fromMaybe [] mbUpdateVals
  case (eitherDbRows, eitherKvRows) of
    (Right allDBRows, Right allKVRows) -> do
      let kvLiveRows = fst allKVRows
          kvDeadRows = snd allKVRows
          kvLiveAndDeadRows = kvLiveRows ++ kvDeadRows
          matchedKVLiveRows = findAllMatching whereClause kvLiveRows
      if isRecachingEnabled && meshCfg.meshEnabled && isLive
        then do
          let uniqueDbRows =  getUniqueDBRes meshCfg.redisKeyPrefix allDBRows kvLiveAndDeadRows
          reCacheDBRowsRes <- reCacheDBRows meshCfg uniqueDbRows
          case reCacheDBRowsRes of
            Left err -> return $ Left $ RedisError (show err <> "Primary key => " <> show (getLookupKeyByPKey meshCfg.redisKeyPrefix <$> uniqueDbRows) <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix <$> uniqueDbRows) <> " in updateKVAndDBResults")
            Right _  -> do
              let allRows = matchedKVLiveRows ++ uniqueDbRows
              sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedis updVals setClause True whereClause) allRows
        else do
          updateOrDelKVRowRes <- if isLive
            then mapM (updateObjectRedis meshCfg meshCfg.kvRedis updVals setClause True whereClause) matchedKVLiveRows
            else mapM (deleteObjectRedis meshCfg meshCfg.kvRedis True whereClause) matchedKVLiveRows
          let kvres = foldEither updateOrDelKVRowRes
          case kvres of
            Left err -> return $ Left err
            Right kvRes -> runUpdateOrDelete setClause kvRes kvLiveAndDeadRows

    (Left err, _) -> pure $ Left (MDBError err)
    (_, Left err) -> pure $ Left err


    where
      runUpdateOrDelete setClause kvres kvLiveAndDeadRows = do
        case (isLive, updateWoReturning) of
          (True, True) -> do
            let updateQuery = DB.updateRows $ sqlUpdate ! #set setClause ! #where_ whereClause
            res <- runQuery dbConf updateQuery
            case res of
                Right _ -> return $ Right []
                Left e -> return $ Left $ MDBError e
          (True, False) -> do
            let updateQuery = DB.updateRowsReturningList $ sqlUpdate ! #set setClause ! #where_ whereClause
            res <- runQuery dbConf updateQuery
            case res of
                Right x -> return $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix x kvLiveAndDeadRows ++ kvres
                Left e  -> return $ Left $ MDBError e
          (False, _) -> do
            res <- deleteAllReturning dbConf whereClause
            case res of
                Right x -> return $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix x kvLiveAndDeadRows ++ kvres
                Left e  -> return $ Left $ MDBError e

-- Helper function to run two async operations in parallel

updateKVAndDBResultsMultiCloud :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    BeamRunner beM,
    Model be table,
    MeshMeta be table,
    TableMappings (table Identity),
    B.HasQBuilder be,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    L.MonadFlow m
  ) => MeshConfig -> Where be table -> Either DBError [table Identity] -> MeshResult ([table Identity], [table Identity]) -> MeshResult ([table Identity], [table Identity]) -> Maybe [(Text, A.Value)] -> Bool -> DBConfig beM -> Maybe [Set be table] -> Bool -> m (MeshResult [table Identity])
updateKVAndDBResultsMultiCloud meshCfg whereClause eitherDbRows primaryKvRows secondaryKvRows mbUpdateVals updateWoReturning dbConf mbSetClause isLive = do
  let setClause = fromMaybe [] mbSetClause
      updVals = fromMaybe [] mbUpdateVals
  case (eitherDbRows, primaryKvRows, secondaryKvRows) of
    (Right allDBRows, Right primaryKVRows, Right secondaryKVRows) -> do
      let primaryKvLiveRows = fst primaryKVRows
          primaryKvDeadRows = snd primaryKVRows
          secondaryKvLiveRows = fst secondaryKVRows
          secondaryKvDeadRows = snd secondaryKVRows
          -- Merge all KV rows for deduplication
          allKvLiveRows = primaryKvLiveRows ++ secondaryKvLiveRows
          allKvDeadRows = primaryKvDeadRows ++ secondaryKvDeadRows
          allKvLiveAndDeadRows = allKvLiveRows ++ allKvDeadRows
          -- Segregate matches by source Redis
          primaryMatchedKVLiveRows = findAllMatching whereClause primaryKvLiveRows
          secondaryMatchedKVLiveRows = findAllMatching whereClause secondaryKvLiveRows
      if isRecachingEnabled && meshCfg.meshEnabled && isLive
        then do
          let uniqueDbRows = getUniqueDBRes meshCfg.redisKeyPrefix allDBRows allKvLiveAndDeadRows
          -- Recache DB rows to primary Redis only
          reCacheDBRowsRes <- reCacheDBRows meshCfg uniqueDbRows
          case reCacheDBRowsRes of
            Left err -> return $ Left $ RedisError (show err <> "Primary key => " <> show (getLookupKeyByPKey meshCfg.redisKeyPrefix <$> uniqueDbRows) <> " Secondary key => " <> show (getSecondaryLookupKeys meshCfg.redisKeyPrefix <$> uniqueDbRows) <> " in updateKVAndDBResultsMultiCloud")
            Right _  -> do
              -- Update primary Redis matches in primary Redis
              primaryUpdateRes <- sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedis updVals setClause True whereClause) primaryMatchedKVLiveRows
              -- Update secondary Redis matches in secondary Redis
              secondaryUpdateRes <- sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedisSecondary updVals setClause True whereClause) secondaryMatchedKVLiveRows
              -- Update DB-only rows in primary Redis
              dbUpdateRes <- sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedis updVals setClause True whereClause) uniqueDbRows
              -- Combine all results
              let allUpdateRes = foldEither [primaryUpdateRes, secondaryUpdateRes, dbUpdateRes]
              case allUpdateRes of
                Left err -> return $ Left err
                Right _ -> do
                  -- Deduplicate results by primary key (in case row exists in both Redis instances)
                  -- uniqueDbRows is already deduplicated against all KV rows, so we just need to deduplicate secondary against primary
                  let uniqueSecondaryRows = getUniqueDBRes meshCfg.redisKeyPrefix secondaryMatchedKVLiveRows primaryMatchedKVLiveRows
                      deduplicatedRows = primaryMatchedKVLiveRows ++ uniqueSecondaryRows ++ uniqueDbRows
                  return $ Right deduplicatedRows
        else do
          -- Update primary Redis matches in primary Redis
          primaryUpdateOrDelRes <- if isLive
            then sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedis updVals setClause True whereClause) primaryMatchedKVLiveRows
            else sequence <$> mapM (deleteObjectRedis meshCfg meshCfg.kvRedis True whereClause) primaryMatchedKVLiveRows
          -- Update secondary Redis matches in secondary Redis
          secondaryUpdateOrDelRes <- if isLive
            then sequence <$> mapM (updateObjectRedis meshCfg meshCfg.kvRedisSecondary updVals setClause True whereClause) secondaryMatchedKVLiveRows
            else sequence <$> mapM (deleteObjectRedis meshCfg meshCfg.kvRedisSecondary True whereClause) secondaryMatchedKVLiveRows
          let allUpdateOrDelRes = foldEither [primaryUpdateOrDelRes, secondaryUpdateOrDelRes]
          case allUpdateOrDelRes of
            Left err -> return $ Left err
            Right kvRes -> do
              -- Flatten the list of lists from sequence
              let flattenedKvRes = concat kvRes
              runUpdateOrDeleteMultiCloud setClause flattenedKvRes allKvLiveAndDeadRows

    (Left err, _, _) -> pure $ Left (MDBError err)
    (_, Left err, _) -> pure $ Left err
    (_, _, Left err) -> pure $ Left err

  where
    runUpdateOrDeleteMultiCloud setClause kvres kvLiveAndDeadRows = do
      case (isLive, updateWoReturning) of
        (True, True) -> do
          let updateQuery = DB.updateRows $ sqlUpdate ! #set setClause ! #where_ whereClause
          res <- runQuery dbConf updateQuery
          case res of
              Right _ -> return $ Right []
              Left e -> return $ Left $ MDBError e
        (True, False) -> do
          let updateQuery = DB.updateRowsReturningList $ sqlUpdate ! #set setClause ! #where_ whereClause
          res <- runQuery dbConf updateQuery
          case res of
              Right x -> return $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix x kvLiveAndDeadRows ++ kvres
              Left e  -> return $ Left $ MDBError e
        (False, _) -> do
          res <- deleteAllReturning dbConf whereClause
          case res of
              Right x -> return $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix x kvLiveAndDeadRows ++ kvres
              Left e  -> return $ Left $ MDBError e


---------------- Find -----------------------
findWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m,
    Show (table Identity)
  ) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult (Maybe (table Identity)))
findWithKVConnector dbConf meshCfg whereClause = do --This function fetches all possible rows and apply where clause on it.
  let shouldSearchInMemoryCache = meshCfg.memcacheEnabled
  (_, res) <- if shouldSearchInMemoryCache
    then do
      inMemResult <- searchInMemoryCache meshCfg dbConf whereClause
      findOneFromDBIfNotFound inMemResult
    else
      kvFetch
  pure res
  where

    findOneFromDBIfNotFound :: (Source, MeshResult [table Identity]) -> m (Source, MeshResult (Maybe (table Identity)))
    findOneFromDBIfNotFound res = case res of
      (source, Right []) -> pure (source, Right Nothing)
      (source, Right rows) -> do
        let matchingRes = findOneMatching whereClause rows
        if isJust matchingRes
          then pure (source, Right matchingRes)
          else (SQL,) <$> findOneFromDB dbConf whereClause
      (source, Left err) -> pure (source, Left err)

    kvFetch :: m (Source, MeshResult (Maybe (table Identity)))
    kvFetch = do
      let isDisabled = meshCfg.kvHardKilled
      if not isDisabled
        then do
          eitherKvRows <- findOneFromRedis meshCfg whereClause
          case eitherKvRows of
            Right ([], []) -> do
              -- (SQL,) <$> findOneFromDB dbConf whereClause
              res <- findOneFromDB dbConf whereClause
              case res of
                Right (Just dbRow) -> do
                  if isCachingDbFindEnabled && meshCfg.meshEnabled
                    then do
                      reCacheDBRowsRes <- createInRedis meshCfg dbRow
                      case reCacheDBRowsRes of
                        Left err -> do
                          L.logErrorT "findWithKVConnector" ("Error while recaching row in redis for " <> tableName @(table Identity) <> "with data" <> show dbRow <> " Error: " <> show err)
                          pure (SQL, Right (Just dbRow))
                        Right _ -> do
                          L.logDebugT "findWithKVConnector" ("Recached row in redis for " <> tableName @(table Identity) <> "with data" <> show dbRow)
                          pure (SQL, Right (Just dbRow))
                    else pure (SQL, Right (Just dbRow))
                Right Nothing -> pure (SQL, Right Nothing)
                Left err -> pure (SQL, Left err)
            Right ([], _) -> do
              L.logInfoT "findWithKVConnector" ("Returning nothing - Row is deleted already for " <> tableName @(table Identity))
              pure (KV, Right Nothing)
            Right (kvLiveRows, _) -> do
              second (mapRight DMaybe.listToMaybe) <$> findFromDBIfMatchingFails meshCfg dbConf whereClause kvLiveRows
            Left err -> pure (KV, Left err)
        else do
          (SQL,) <$> findOneFromDB dbConf whereClause

findOneFromKvRedis :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m
  ) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult (Maybe (table Identity)))
findOneFromKvRedis dbConf meshCfg whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  (_, res) <- if not isDisabled
    then do
      eitherKvRows <- findOneFromRedis meshCfg whereClause
      case eitherKvRows of
        Right ([], []) -> do
          pure (KV, Right Nothing)
        Right ([], _) -> do
          L.logInfoT "findOneFromKvRedis" ("Returning nothing - Row is deleted already for " <> tableName @(table Identity))
          pure (KV, Right Nothing)
        Right (kvLiveRows, _) -> do
          let matchingData = findAllMatching whereClause kvLiveRows
          pure (KV, Right (DMaybe.listToMaybe matchingData))
        Left err -> pure (KV, Left err)
    else do
      (SQL,) <$> findOneFromDB dbConf whereClause
  pure res


findFromDBIfMatchingFails :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m) =>
  MeshConfig ->
  DBConfig beM ->
  Where be table ->
  [table Identity] ->
  m (Source, MeshResult [table Identity])
findFromDBIfMatchingFails meshCfg dbConf whereClause kvRows = do
  case findAllMatching whereClause kvRows of -- For solving partial data case - One row in SQL and one in DB
    [] -> do
      dbRes <- findOneFromDB dbConf whereClause
      case dbRes of
        Right (Just dbRow) -> do
          let kvPkeys = map (getLookupKeyByPKey meshCfg.redisKeyPrefix) kvRows
          if getLookupKeyByPKey meshCfg.redisKeyPrefix dbRow `notElem` kvPkeys
            then pure (SQL, Right [dbRow])
            else pure (KV, Right [])
        Left err           -> pure (SQL, Left err)
        {- Below source cannot be determined as there can be 2 possiblities
           1. Row is in KV but matching failed because of some column like status
           2. Row is in KV for other clause (Eg. merchantId) but not for required clause and also not in SQL
           Source as KV can be more misleading, therefore returning source as SQL -}
        _                  -> pure (SQL, Right [])
    xs -> pure (KV, Right xs)

-- TODO: Once record matched in redis stop and return it
findOneFromRedis :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m
  ) =>
  MeshConfig -> Where be table -> m (MeshResult ([table Identity], [table Identity]))
findOneFromRedis meshCfg whereClause = do
  let keyAndValueCombinations = getFieldsAndValuesFromClause meshModelTableEntityDescriptor (And whereClause)
      andCombinations = map (uncurry zip . applyFPair (map (T.intercalate "_") . sortOn (Down . length) . nonEmptySubsequences) . unzip . sort) keyAndValueCombinations
      modelName = tableName @(table Identity)
      keyHashMap = keyMap @(table Identity)
      andCombinationsFiltered = mkUniq $ filterPrimaryAndSecondaryKeys keyHashMap <$> andCombinations
      secondaryKeyLength = sum $ getSecondaryKeyLength keyHashMap <$> andCombinationsFiltered
      clusterName = if meshCfg.kvRedis == meshCfg.kvRedisSecondary then "secondaryCluster" else "primaryCluster"
  
  Metrics.withKVLatencyMetric "REDIS_FIND_ONE" modelName clusterName $ do
    eitherKeyRes <- mapM (getPrimaryKeyFromFieldsAndValues modelName meshCfg keyHashMap) andCombinationsFiltered
    result <- case foldEither eitherKeyRes of
      Right keyRes -> do
        let lenKeyRes = lengthOfLists keyRes
        allRowsRes <- foldEither <$> mapM (getDataFromPKeysRedis meshCfg.kvRedis) (mkUniq keyRes)
        case allRowsRes of
          Right allRowsResPairList -> do
            let (allRowsResLiveListOfList, allRowsResDeadListOfList) = unzip allRowsResPairList
                primaryLiveRows = concat allRowsResLiveListOfList
                primaryDeadRows = concat allRowsResDeadListOfList
                total_length = secondaryKeyLength + lenKeyRes
            Metrics.incrementRedisCallMetric "REDIS_FIND_ONE" modelName total_length (total_length > redisCallsSoftLimit ) (total_length > redisCallsHardLimit )
            -- Apply WHERE clause to primary results to check if we have any matches
            let primaryMatching = findAllMatching whereClause primaryLiveRows
            -- Check secondary cloud Redis if:
            -- 1. No matching rows in primary (even if raw rows exist)
            -- 2. No dead rows (deleted data should not trigger fallback)
            -- 3. Mesh and secondary enabled
            if null primaryMatching && null primaryDeadRows && meshCfg.meshEnabled && meshCfg.secondaryRedisEnabled
              then do
                Metrics.withKVLatencyMetric "REDIS_FIND_ONE" modelName "secondaryCluster" $ do
                  secondaryRowsRes <- foldEither <$> mapM (getDataFromPKeysRedis meshCfg.kvRedisSecondary) (mkUniq keyRes)
                  case secondaryRowsRes of
                    Right secondaryRowsResPairList -> do
                      let (secondaryLiveListOfList, secondaryDeadListOfList) = unzip secondaryRowsResPairList
                      Metrics.incrementRedisCallMetric "REDIS_FIND_ONE_SECONDARY" modelName total_length (total_length > redisCallsSoftLimit ) (total_length > redisCallsHardLimit )
                      return (Right (concat secondaryLiveListOfList ++ primaryLiveRows, concat secondaryDeadListOfList ++ primaryDeadRows), True)
                    Left err -> return (Left err, True)
              else
                return (Right (primaryLiveRows, primaryDeadRows), False)
          Left err -> return (Left err, False)
      Left err -> pure (Left err, False)
    
    pure $ fst result

findOneFromDB :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m
  ) =>
  DBConfig beM -> Where be table -> m (MeshResult (Maybe (table Identity)))
findOneFromDB dbConf whereClause = do
  let findQuery = DB.findRow (sqlSelect ! #where_ whereClause ! defaults)
  mapLeft MDBError <$> runQuery dbConf findQuery

compareCols :: (Ord value) => (table Identity -> value) -> Bool -> table Identity -> table Identity -> Ordering
compareCols col isAsc r1 r2 = if isAsc then compare (col r1) (col r2) else compare (col r2) (col r1)

findAllWithOptionsHelper :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  Maybe (OrderBy table) ->
  Maybe Int ->
  Maybe Int ->
  m (MeshResult [table Identity])
findAllWithOptionsHelper dbConf meshCfg whereClause orderBy mbLimit mbOffset = do
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows) <- callKVKVAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
          case extractMultiCloudKVRows primaryKvRows secondaryKvRows of
            Right (primaryKvLiveRows, primaryKvDeadRows, secondaryKvLiveRows, secondaryKvDeadRows) -> do
              let -- Merge all KV rows
                  allKvLiveRows = primaryKvLiveRows ++ secondaryKvLiveRows
                  allKvDeadRows = primaryKvDeadRows ++ secondaryKvDeadRows
                  -- Match and deduplicate rows from both Redis instances
                  matchedKVLiveRows = matchAndDeduplicateKVRows meshCfg whereClause primaryKvLiveRows secondaryKvLiveRows
                  matchedKVDeadRows = deduplicateKVDeadRows meshCfg primaryKvDeadRows secondaryKvDeadRows
                  offset = fromMaybe 0 mbOffset
                  shift = length matchedKVLiveRows + length matchedKVDeadRows
                  -- Desc: KV rows (latest) occupy positions 0..shift-1 in DB too;
                  -- DB's own offset already accounts for them, so no reduction needed.
                  -- Asc: KV rows are at the high end; reducing offset is safe.
                  updatedOffset = case orderBy of
                    Just (Desc _) -> offset
                    _             -> max (offset - shift) 0
                  findAllQueryUpdated = DB.findRows (sqlSelect'
                    ! #where_ whereClause
                    ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
                    ! #limit ((shift +) <$> mbLimit)
                    ! #offset (Just updatedOffset)
                    ! defaults)
              dbRes <- runQuery dbConf findAllQueryUpdated
              case dbRes of
                Left err -> pure $ Left $ MDBError err
                Right [] -> pure $ Right $ applyOptions offset matchedKVLiveRows
                Right dbRows -> do
                  let allKVRows = allKvLiveRows ++ allKvDeadRows
                      mergedRows = matchedKVLiveRows ++ getUniqueDBRes meshCfg.redisKeyPrefix dbRows allKVRows
                  if isJust mbOffset
                    then pure $ Right $ applyOptions (min offset shift) mergedRows
                    else pure $ Right $ applyOptions 0 mergedRows
            Left err -> pure $ Left err
        else do
          -- Original single Redis logic
          kvRes <- redisFindAll meshCfg whereClause
          case kvRes of
            Right kvRows -> do
              let matchedKVLiveRows = findAllMatching whereClause (fst kvRows)
                  matchedKVDeadRows = snd kvRows
                  offset = fromMaybe 0 mbOffset
                  shift = length matchedKVLiveRows + length matchedKVDeadRows
                  updatedOffset = case orderBy of
                    Just (Desc _) -> offset
                    _             -> max (offset - shift) 0
                  findAllQueryUpdated = DB.findRows (sqlSelect'
                    ! #where_ whereClause
                    ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
                    ! #limit ((shift +) <$> mbLimit)
                    ! #offset (Just updatedOffset)
                    ! defaults)
              dbRes <- runQuery dbConf findAllQueryUpdated
              case dbRes of
                Left err -> pure $ Left $ MDBError err
                Right [] -> pure $ Right $ applyOptions offset matchedKVLiveRows
                Right dbRows -> do
                  let mergedRows = matchedKVLiveRows ++ getUniqueDBRes meshCfg.redisKeyPrefix dbRows (snd kvRows ++ fst kvRows)
                  if isJust mbOffset
                    then pure $ Right $ applyOptions (min offset shift) mergedRows
                    else pure $ Right $ applyOptions 0 mergedRows
            Left err -> pure $ Left err
    else do
      let findAllQuery = DB.findRows (sqlSelect'
            ! #where_ whereClause
            ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
            ! #limit mbLimit
            ! #offset mbOffset
            ! defaults)
      mapLeft MDBError <$> runQuery dbConf findAllQuery
  where
      applyOptions :: Int -> [table Identity] -> [table Identity]
      applyOptions shift rows = do
        let resWithoutLimit = case orderBy of
                          Nothing -> drop shift rows
                          Just res -> do
                            let cmp = case res of
                                  Asc col -> compareCols (fromColumnar' . col . columnize) True
                                  Desc col -> compareCols (fromColumnar' . col . columnize) False
                            (drop shift . sortBy cmp) rows
        maybe resWithoutLimit (`take` resWithoutLimit) mbLimit

      calculateLeftFelledRedisEntries :: [table Identity] -> [table Identity] -> Int
      calculateLeftFelledRedisEntries kvRows dbRows = do
        case orderBy of
          Just (Asc col) -> do
            let dbMn = minimum $ map (fromColumnar' . col . columnize) dbRows
            length $ filter (\r -> dbMn > fromColumnar' (col $ columnize r)) kvRows
          Just (Desc col) -> do
            let dbMx = maximum $ map (fromColumnar' . col . columnize) dbRows
            length $ filter (\r -> dbMx < fromColumnar' (col $ columnize r)) kvRows
          Nothing -> 0


findAllFromKvRedis :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    B.HasQBuilder be,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m
  ) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  Maybe (OrderBy table) ->
  m (MeshResult [table Identity])
findAllFromKvRedis dbConf meshCfg whereClause orderBy = do
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows) <- callKVKVAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
          case extractMultiCloudKVRows primaryKvRows secondaryKvRows of
            Right (primaryKvLiveRows, _, secondaryKvLiveRows, _) -> do
              let matchedKVLiveRows = matchAndDeduplicateKVRows meshCfg whereClause primaryKvLiveRows secondaryKvLiveRows
              pure $ Right $ applyOptions matchedKVLiveRows
            Left err -> pure $ Left err
        else do
          -- Original single Redis logic
          kvRes <- redisFindAll meshCfg whereClause
          case kvRes of
            Right kvRows -> do
              let matchedKVLiveRows = findAllMatching whereClause (fst kvRows)
              pure $ Right $ applyOptions matchedKVLiveRows
            Left err -> pure $ Left err
    else do
      let findAllQuery = DB.findRows (sqlSelect'
            ! #where_ whereClause
            ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
            ! defaults)
      mapLeft MDBError <$> runQuery dbConf findAllQuery
    where 
      applyOptions = case orderBy of
        Just (Asc col) -> sortBy (compareCols (fromColumnar' . col . columnize) True)
        Just (Desc col) -> sortBy (compareCols (fromColumnar' . col . columnize) False)
        Nothing -> id

-- Need to recheck offset implementation
findAllWithOptionsKVConnector :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  OrderBy table ->
  Maybe Int ->
  Maybe Int ->
  m (MeshResult [table Identity])
findAllWithOptionsKVConnector dbConf meshCfg whereClause orderBy mbLimit mbOffset = do
  findAllWithOptionsHelper dbConf meshCfg whereClause (Just orderBy) mbLimit mbOffset

-- Need to recheck offset implementation
findAllWithOptionsKVConnector' :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    Serialize.Serialize (table Identity),
    Show (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  Maybe Int ->
  Maybe Int ->
  m (MeshResult [table Identity])
findAllWithOptionsKVConnector' dbConf meshCfg whereClause  =
  findAllWithOptionsHelper dbConf meshCfg whereClause Nothing

findAllWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult [table Identity])
findAllWithKVConnector dbConf meshCfg whereClause = do
  let findAllQuery = DB.findRows (sqlSelect ! #where_ whereClause ! defaults)
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis, and DB in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows, dbRows) <- callMultiAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
            (findAllSql dbConf whereClause)
          case (primaryKvRows, secondaryKvRows, dbRows) of
            (Right _, Right _, Right dbRes) -> do
              -- Both Redis results are Right, so extractMultiCloudKVRows will always succeed
              case extractMultiCloudKVRows primaryKvRows secondaryKvRows of
                Right (primaryKvLiveRows, primaryKvDeadRows, secondaryKvLiveRows, secondaryKvDeadRows) -> do
                  let -- Merge all KV rows
                      allKvLiveRows = primaryKvLiveRows ++ secondaryKvLiveRows
                      allKvDeadRows = primaryKvDeadRows ++ secondaryKvDeadRows
                      allKVRows = allKvLiveRows ++ allKvDeadRows
                      -- Match and deduplicate rows from both Redis instances
                      allMatchedKVLiveRows = matchAndDeduplicateKVRows meshCfg whereClause primaryKvLiveRows secondaryKvLiveRows
                      -- Get unique DB rows (not in any KV rows)
                      uniqueDbRows = getUniqueDBRes meshCfg.redisKeyPrefix dbRes allKVRows
                  pure $ Right $ allMatchedKVLiveRows ++ uniqueDbRows
                Left err -> return $ Left err  -- Should never happen since both inputs are Right, but needed for completeness
            (Left err, _, _) -> return $ Left err
            (_, Left err, _) -> return $ Left err
            (_, _, Left err) -> return $ Left $ MDBError err
        else do
          -- Original single Redis logic          (kvRows, dbRows) <- callKVDBAsync (redisFindAll meshCfg whereClause) (findAllSql dbConf whereClause)
          case (kvRows, dbRows) of
            (Right kvRes, Right dbRes) -> do
              let matchedKVLiveRows = findAllMatching whereClause (fst kvRes)
                  allKVRows = fst kvRes ++ snd kvRes
              pure $ Right $ matchedKVLiveRows ++ getUniqueDBRes meshCfg.redisKeyPrefix dbRes allKVRows
            (Left err, _) -> return $ Left err
            (_, Left err) -> return $ Left $ MDBError err
    else do
      mapLeft MDBError <$> runQuery dbConf findAllQuery

findAllWithKVAndConditionalDBInternal :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  Maybe (OrderBy table) ->
  m (MeshResult [table Identity])
findAllWithKVAndConditionalDBInternal dbConf meshCfg whereClause orderBy = do
  let isDisabled = meshCfg.kvHardKilled
  
  if not isDisabled
    then do
      -- Find from both Redis instances if secondary is enabled
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Find from primary Redis, secondary Redis in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows) <- callKVKVAsync 
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
          case extractMultiCloudKVRows primaryKvRows secondaryKvRows of
            Right (primaryKvLiveRows, primaryKvDeadRows, secondaryKvLiveRows, secondaryKvDeadRows) -> do
              let matchedKVLiveRows = matchAndDeduplicateKVRows meshCfg whereClause primaryKvLiveRows secondaryKvLiveRows
              if not (null matchedKVLiveRows)
                then do
                  case orderBy of
                      Nothing -> pure $ Right matchedKVLiveRows
                      Just res -> do
                        let cmp = case res of
                              Asc col -> compareCols (fromColumnar' . col . columnize) True
                              Desc col -> compareCols (fromColumnar' . col . columnize) False
                        pure $ Right (sortBy cmp matchedKVLiveRows)
                else do
                  let findAllQueryUpdated = DB.findRows (sqlSelect'
                          ! #where_ whereClause
                          ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
                          ! defaults)
                  dbRes <- runQuery dbConf findAllQueryUpdated
                  case dbRes of
                    Right dbRows -> do
                      let allKvDeadRows = primaryKvDeadRows ++ secondaryKvDeadRows
                      pure $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix dbRows allKvDeadRows
                    Left err     -> return $ Left $ MDBError err
            Left err -> return $ Left err
        else do
          -- Original single Redis logic
          kvRes <- redisFindAll meshCfg whereClause
          case kvRes of
            Right kvRows -> do
              let matchedKVLiveRows = findAllMatching whereClause (fst kvRows)
              if not (null matchedKVLiveRows)
                then do
                  case orderBy of
                      Nothing -> pure $ Right matchedKVLiveRows
                      Just res -> do
                        let cmp = case res of
                              Asc col -> compareCols (fromColumnar' . col . columnize) True
                              Desc col -> compareCols (fromColumnar' . col . columnize) False
                        pure $ Right (sortBy cmp matchedKVLiveRows)
                else do
                  let findAllQueryUpdated = DB.findRows (sqlSelect'
                          ! #where_ whereClause
                          ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
                          ! defaults)
                  dbRes <- runQuery dbConf findAllQueryUpdated
                  case dbRes of
                    Right dbRows -> pure $ Right $ getUniqueDBRes meshCfg.redisKeyPrefix dbRows (snd kvRows)
                    Left err     -> return $ Left $ MDBError err
            Left err -> return $ Left err
    else do
      let findAllQuery = DB.findRows (sqlSelect'
              ! #where_ whereClause
              ! #orderBy (if isJust orderBy then Just [DMaybe.fromJust orderBy] else Nothing)
              ! defaults)
      mapLeft MDBError <$> runQuery dbConf findAllQuery


redisFindAll :: forall be table beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    KVConnector (table Identity),
    FromJSON (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  MeshConfig ->
  Where be table ->
  m (MeshResult ([table Identity], [table Identity]))
redisFindAll meshCfg whereClause = do
  let keyAndValueCombinations = getFieldsAndValuesFromClause meshModelTableEntityDescriptor (And whereClause)
      andCombinations = map (uncurry zip . applyFPair (map (T.intercalate "_") . sortOn (Down . length) . nonEmptySubsequences) . unzip . sort) keyAndValueCombinations
      modelName = tableName @(table Identity)
      keyHashMap = keyMap @(table Identity)
      andCombinationsFiltered = mkUniq $ filterPrimaryAndSecondaryKeys keyHashMap <$> andCombinations
      secondaryKeyLength = sum $ getSecondaryKeyLength keyHashMap <$> andCombinationsFiltered
      clusterName = if meshCfg.kvRedis == meshCfg.kvRedisSecondary then "secondaryCluster" else "primaryCluster"
  
  Metrics.withKVLatencyMetric "REDIS_FIND_ALL" modelName clusterName $ do
    eitherKeyRes <- mapM (getPrimaryKeyFromFieldsAndValues modelName meshCfg keyHashMap) andCombinationsFiltered
    result <- case foldEither eitherKeyRes of
      Right keyRes -> do
        let allKeys = concat keyRes
        let lenKeyRes = length allKeys
        allRowsRes <- (if isPipeliningEnabled then foldEither <$> mapM (getDataFromPKeysRedis' meshCfg) [(mkUniq allKeys)] else foldEither <$> mapM (getDataFromPKeysRedis meshCfg.kvRedis) (mkUniq keyRes))
        let total_length = secondaryKeyLength + lenKeyRes
        case allRowsRes of
          Right allRowsResPairList -> do
            let (allRowsResLiveListOfList, allRowsResDeadListOfList) = unzip allRowsResPairList
            Metrics.incrementRedisCallMetric "REDIS_FIND_ALL" modelName total_length (total_length > redisCallsSoftLimit ) (total_length > redisCallsHardLimit )
            return $ Right (concat allRowsResLiveListOfList, concat allRowsResDeadListOfList)
          Left err -> return $ Left err
      Left err -> pure $ Left err
    
    pure result

deleteObjectRedis :: forall table be beM m.
  ( HasCallStack,
    BeamRuntime be beM,
    BeamRunner beM,
    Model be table,
    TableMappings (table Identity),
    MeshMeta be table,
    B.HasQBuilder be,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    -- Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  MeshConfig -> Text -> Bool -> Where be table -> table Identity -> m (MeshResult (table Identity))
deleteObjectRedis meshCfg redisConn addPrimaryKeyToWhereClause whereClause obj = do
  time <- fromIntegral <$> L.getCurrentDateInMillis
  let pKeyText  = getLookupKeyByPKey meshCfg.redisKeyPrefix obj
      shard     = getShardedHashTag meshCfg.tableShardModRange pKeyText
      pKey      = fromString . T.unpack $ pKeyText <> shard
      deleteCmd = if addPrimaryKeyToWhereClause
                    then getDbDeleteCommandJsonWithPrimaryKey  (getTableName @(table Identity)) obj whereClause
                    else getDbDeleteCommandJson  (getTableName @(table Identity)) whereClause

  qCmd <- if isCompressionAllowed 
          then do getDeleteQueryWithCompression (pKeyText <> shard) time meshCfg.meshDBName deleteCmd (getTableMappings @(table Identity)) (getTableName @(table Identity))
          else do pure $ BSL.toStrict $ A.encode $ getDeleteQuery (pKeyText <> shard) time meshCfg.meshDBName deleteCmd (getTableMappings @(table Identity)) 

  kvDbRes <- L.runKVDB redisConn $ L.multiExecWithHash (encodeUtf8 shard) $ do
    _ <- L.xaddTx
          (encodeUtf8 (meshCfg.ecRedisDBStream <> shard))
          L.AutoID
          [("command",  qCmd)]
    L.setexTx pKey meshCfg.redisTtl (BSL.toStrict $ Encoding.encodeDead $ Encoding.encode_ meshCfg.cerealEnabled obj)
  case kvDbRes of
    Left err -> return . Left $ RedisError (show err <> " for key " <> show pKey <> "in DeleteObjectRedis")
    Right _  -> do
      when meshCfg.memcacheEnabled $ pushToInMemConfigStream meshCfg ImcDelete obj
      return $ Right obj

reCacheDBRows :: forall table m.
  ( HasCallStack,
    KVConnector (table Identity),
    FromJSON (table Identity),
    ToJSON (table Identity),
    Serialize.Serialize (table Identity),
    -- Show (table Identity), --debugging purpose
    L.MonadFlow m
  ) =>
  MeshConfig ->
  [table Identity] ->
  m (Either KVDBReply [[Bool]])
reCacheDBRows meshCfg dbRows = do
  reCacheRes <- mapM (\obj -> do
      let pKeyText = getLookupKeyByPKey meshCfg.redisKeyPrefix obj
          shard = getShardedHashTag meshCfg.tableShardModRange pKeyText
          pKey = fromString . T.unpack $ pKeyText <> shard
      res <- mapM (\secIdx -> do -- Recaching Skeys in redis
          let sKey = fromString . T.unpack $ secIdx
          res1 <- L.runKVDB meshCfg.kvRedis $ L.sadd sKey [pKey]
          case res1 of
            Left err -> return $ Left err
            Right _  ->
              L.runKVDB meshCfg.kvRedis $  L.expire sKey meshCfg.redisTtl
        ) $ getSecondaryLookupKeys meshCfg.redisKeyPrefix obj
      return $ sequence res
    ) dbRows
  return $ sequence reCacheRes

deleteWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    B.HasQBuilder be,
    KVConnector (table Identity),
    TableMappings (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    Show (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult ())
deleteWithKVConnector dbConf meshCfg whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  (_, res) <- if not isDisabled
    then do
      (\delRes -> (fst delRes, mapRight (const ()) (snd delRes))) <$> modifyOneKV dbConf dbConf meshCfg whereClause Nothing True False
    else do
      let deleteQuery = DB.deleteRows $ sqlDelete ! #where_ whereClause
      res <- runQuery dbConf deleteQuery
      (SQL,) <$> case res of
        Left err -> return $ Left $ MDBError err
        Right re -> do
          if meshCfg.memcacheEnabled
            then
              searchInMemoryCache meshCfg dbConf whereClause >>= (snd >>> \case
                Left e -> return $ Left e
                Right rs -> do
                  mapM_ (pushToInMemConfigStream meshCfg ImcDelete) rs
                  return $ Right re)
            else do
                return $ Right re

  pure res

deleteReturningWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    B.HasQBuilder be,
    KVConnector (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    TableMappings (table Identity),
    Show (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult (Maybe (table Identity)))
deleteReturningWithKVConnector dbConf meshCfg whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  (_, res) <- if not isDisabled
    then do
      modifyOneKV dbConf dbConf meshCfg whereClause Nothing False False
    else do
      res <- deleteAllReturning dbConf whereClause
      (SQL,) <$> case res of
        Left err  -> return $ Left $ MDBError err
        Right []  -> return $ Right Nothing
        Right [r] -> do
          when meshCfg.memcacheEnabled $ pushToInMemConfigStream meshCfg ImcDelete r
          return $ Right (Just r)
        Right rs   -> do
          when meshCfg.memcacheEnabled $ mapM_ (pushToInMemConfigStream meshCfg ImcDelete) rs
          return $ Left $ MUpdateFailed "SQL delete returned more than one record"
  pure res

deleteAllReturningWithKVConnector :: forall be table beM m.
  ( HasCallStack,
    SqlReturning beM be,
    BeamRuntime be beM,
    Model be table,
    MeshMeta be table,
    B.HasQBuilder be,
    KVConnector (table Identity),
    TableMappings (table Identity),
    ToJSON (table Identity),
    FromJSON (table Identity),
    Show (table Identity),
    Serialize.Serialize (table Identity),
    L.MonadFlow m, B.HasQBuilder be, BeamRunner beM) =>
  DBConfig beM ->
  MeshConfig ->
  Where be table ->
  m (MeshResult [table Identity])
deleteAllReturningWithKVConnector dbConf meshCfg whereClause = do
  let isDisabled = meshCfg.kvHardKilled
  if not isDisabled
    then do
      if meshCfg.secondaryRedisEnabled && meshCfg.meshEnabled
        then do
          -- Multi-cloud: find from both Redis instances and DB in parallel
          let (primaryOnlyCfg, secondaryAsPrimaryCfg) = createMultiCloudConfigs meshCfg
          (primaryKvRows, secondaryKvRows, dbRows) <- callMultiAsync
            (redisFindAll primaryOnlyCfg whereClause)
            (redisFindAll secondaryAsPrimaryCfg whereClause)
            (findAllSql dbConf whereClause)
          updateKVAndDBResultsMultiCloud meshCfg whereClause dbRows primaryKvRows secondaryKvRows Nothing False dbConf Nothing False
        else do
          -- Original single Redis logic
          (kvResult,dbRows) <- callKVDBAsync (redisFindAll meshCfg whereClause) (findAllSql dbConf whereClause) 
          updateKVAndDBResults meshCfg whereClause dbRows kvResult Nothing False dbConf Nothing False
    else do
      res <- deleteAllReturning dbConf whereClause
      case res of
        Left err -> return $ Left $ MDBError err
        Right re -> do
          when meshCfg.memcacheEnabled $ mapM_ (pushToInMemConfigStream meshCfg ImcDelete) re
          return $ Right re
