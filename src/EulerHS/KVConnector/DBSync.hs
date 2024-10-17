
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module EulerHS.KVConnector.DBSync where

import           EulerHS.Prelude
import           EulerHS.KVConnector.Types (DBCommandVersion (..),  KVConnector (mkSQLObject), MeshMeta(..))
import           EulerHS.KVConnector.Utils (jsonKeyValueUpdates, getPKeyAndValueList, meshModelTableEntityDescriptor, toPSJSON)
import qualified Data.Aeson as A
import           Data.Aeson ((.=))
import qualified Data.Aeson.Key as AKey
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.Text as T
import qualified Database.Beam as B
import qualified Database.Beam.Schema.Tables as B
import           Sequelize (Model, Set, Where, Clause(..), Term(..), Column, fromColumnar', columnize)
import           Sequelize.SQLObject (ToSQLObject (convertToSQLObject))
import           Text.Casing (pascal)

-- For storing DBCommands in stream

type Tag = Text

type DBName = Text

getCreateQuery :: (KVConnector (table Identity),ToJSON(table Identity)) => Text -> Tag -> Double -> DBName -> table Identity -> [(String, String)] -> Maybe Text -> A.Value
getCreateQuery model tag timestamp dbName dbObject mappings compressedObj = do
  A.object
    ([ "contents_v2" .= A.object
        [  "cmdVersion" .= V2
        ,  "tag" .= tag
        ,  "timestamp" .= timestamp
        ,  "dbName" .= dbName
        ,  "command" .= A.object
            [ "contents" .= mkSQLObject dbObject,
              "tag" .= ((T.pack . pascal . T.unpack) model <> "Object")
            ]
        ]
    , "mappings" .= A.toJSON (AKM.fromList $ (\(k, v) -> (AKey.fromText $ T.pack k, v)) <$> mappings)
    , "modelObject" .= dbObject
    , "tag" .= ("Create" :: Text)
    ] <> [ "compressedObj" .= compressedObj | isJust compressedObj])


getCreateQueryForCompression :: (KVConnector (table Identity),ToJSON(table Identity)) => Text -> Tag -> Double -> DBName -> table Identity -> [(String, String)] -> A.Value
getCreateQueryForCompression model tag timestamp dbName dbObject mappings = do
  A.object
    [ "contents_v2" .= A.object
        [  "cmdVersion" .= V2
        ,  "tag" .= tag
        ,  "timestamp" .= timestamp
        ,  "dbName" .= dbName
        ,  "command" .= A.object
            [ "contents" .= mkSQLObject dbObject,
              "tag" .= ((T.pack . pascal . T.unpack) model <> "Object")
            ]
        ]
    , "mappings" .= A.toJSON (AKM.fromList $ (\(k, v) -> (AKey.fromText $ T.pack k, v)) <$> mappings)
    , "modelObject" .= dbObject
    , "tag" .= ("Create" :: Text)
    ]

-- | This will take updateCommand from getDbUpdateCommandJson and returns Aeson value of Update DBCommand
getUpdateQuery :: Tag -> Double -> DBName -> A.Value -> [(String, String)] -> A.Value -> A.Value
getUpdateQuery tag timestamp dbName updateCommandV2 mappings updatedModel = A.object
    [ "contents_v2" .= A.object
        [  "cmdVersion" .= V2
        ,  "tag" .= tag
        ,  "timestamp" .= timestamp
        ,  "dbName" .= dbName
        ,  "command" .= updateCommandV2
        ]
    , "mappings" .= A.toJSON (AKM.fromList $ (\(k, v) -> (AKey.fromText $ T.pack k, v)) <$> mappings)
    , "updatedModel" .= A.toJSON updatedModel
    , "tag" .= ("Update" :: Text)
    ]

getDbUpdateCommandJson :: forall be table. (Model be table, MeshMeta be table) => Text -> [Set be table] -> Where be table -> A.Value
getDbUpdateCommandJson model setClauses whereClause = A.object
  [ "contents" .= A.toJSON
      [ updValToJSON . (toPSJSON @be @table) <$> upd
      , [whereClauseToJson whereClause]
      ]
  , "tag" .= ((T.pack . pascal . T.unpack) model <> "Options")
  ]
  where
      upd = jsonKeyValueUpdates V2 setClauses

getDbUpdateCommandJsonWithPrimaryKey :: forall be table. (KVConnector (table Identity), Model be table, MeshMeta be table, A.ToJSON (table Identity)) => Text -> [Set be table] -> table Identity -> Where be table -> A.Value
getDbUpdateCommandJsonWithPrimaryKey model setClauses table whereClause = A.object
  [ "contents" .= A.toJSON
      [ updValToJSON  . (toPSJSON @be @table) <$> upd
      , [(whereClauseJsonWithPrimaryKey @be) table $ whereClauseToJson  whereClause]
      ]
  , "tag" .= ((T.pack . pascal . T.unpack) model <> "Options")
  ]
  where
      upd = jsonKeyValueUpdates V2 setClauses

whereClauseJsonWithPrimaryKey :: forall be table. (HasCallStack, KVConnector (table Identity), A.ToJSON (table Identity), MeshMeta be table) =>  table Identity -> A.Value -> A.Value
whereClauseJsonWithPrimaryKey table whereClause = do
  let clauseContentsField =  "clauseContents"
  case whereClause of
    A.Object o ->
      let mbClause = AKM.lookup clauseContentsField o
      in case mbClause of
          Just clause ->
            let pKeyValueList = getPKeyAndValueList  table
                modifiedKeyValueList = modifyKeyValue <$> pKeyValueList
                andOfKeyValueList = A.toJSON $ AKM.singleton "$and" $ A.toJSON modifiedKeyValueList
                modifiedClause = A.toJSON $ AKM.singleton "$and" $ A.toJSON [clause, andOfKeyValueList]
                modifiedObject = AKM.insert clauseContentsField modifiedClause o
            in A.toJSON modifiedObject
          Nothing -> error $ "Invalid whereClause, contains no item " <> AKey.toText clauseContentsField
    _ -> error "Cannot modify whereClause that is not an Object"

  where
    modifyKeyValue :: (Text, A.Value) -> A.Value
    modifyKeyValue (key, value) = A.toJSON $ AKM.singleton (AKey.fromText key) (snd $ (toPSJSON @be @table) (key, value))

getDeleteQuery :: Tag -> Double -> DBName -> A.Value -> [(String, String)]  -> A.Value
getDeleteQuery tag timestamp dbName deleteCommandV2 mappings = A.object
  [ "contents_v2" .= A.object
      [  "cmdVersion" .= V2
      ,  "tag" .= tag
      ,  "timestamp" .= timestamp
      ,  "dbName" .= dbName
      ,  "command" .= deleteCommandV2
      ]
  , "mappings" .= A.toJSON (AKM.fromList $ (\(k, v) -> (AKey.fromText $ T.pack k, v)) <$> mappings)
  , "tag" .= ("Delete" :: Text)
  ]

getDbDeleteCommandJson :: forall be table. (Model be table, MeshMeta be table) => Text -> Where be table -> A.Value
getDbDeleteCommandJson model whereClause = A.object
  [ "contents" .= whereClauseToJson whereClause
  , "tag" .= ((T.pack . pascal . T.unpack) model <> "Options")
  ]

getDbDeleteCommandJsonWithPrimaryKey :: forall be table. (HasCallStack, KVConnector (table Identity), Model be table, MeshMeta be table, A.ToJSON (table Identity)) => Text -> table Identity -> Where be table -> A.Value
getDbDeleteCommandJsonWithPrimaryKey model table whereClause = A.object
  [ "contents" .= (whereClauseJsonWithPrimaryKey @be)  table (whereClauseToJson  whereClause)
  , "tag" .= ((T.pack . pascal . T.unpack) model <> "Options")
  ]

updValToJSON :: (Text, A.Value) -> A.Value
updValToJSON (k, v) = A.object [ "key" .= k, "value" .= v ]

whereClauseToJson :: (Model be table, MeshMeta be table) => Where be table -> A.Value
whereClauseToJson whereClause = A.object
    [ "clauseTag" .= ("where" :: Text)
    , "clauseContents" .= modelEncodeWhere whereClause
    ]

modelEncodeWhere ::
  forall be table.
  (Model be table, MeshMeta be table) =>
  Where be table ->
  A.Object
modelEncodeWhere = encodeWhere meshModelTableEntityDescriptor

encodeWhere ::
  forall be table.
  (B.Beamable table, MeshMeta be table) =>
  B.DatabaseEntityDescriptor be (B.TableEntity table) ->
  Where be table ->
  A.Object
encodeWhere dt = encodeClause dt . And

encodeClause ::
  forall be table.
  (B.Beamable table, MeshMeta be table) =>
  B.DatabaseEntityDescriptor be (B.TableEntity table) ->
  Clause be table ->
  A.Object
encodeClause dt w =
  let foldWhere' = \case
        And cs -> foldAnd cs
        Or cs -> foldOr cs
        Is column val -> foldIs column val
      foldAnd = \case
        [] -> AKM.empty
        [x] -> foldWhere' x
        xs -> AKM.singleton "$and" (A.toJSON $ map foldWhere' xs)
      foldOr = \case
        [] -> AKM.empty
        [x] -> foldWhere' x
        xs -> AKM.singleton "$or" (A.toJSON $ map foldWhere' xs)
      foldIs :: (A.ToJSON a, ToSQLObject a) => Column table value -> Term be a -> A.Object
      foldIs column term =
        let key =
              B._fieldName . fromColumnar' . column . columnize $
                B.dbTableSettings dt
         in AKM.singleton (AKey.fromText key) $ (encodeTerm @table) key term
   in foldWhere' w

encodeTerm :: forall table be value. (A.ToJSON value, MeshMeta be table, ToSQLObject value) => Text -> Term be value -> A.Value
encodeTerm  key = \case
  In vals -> array "$in" (modifyToPsFormat <$> vals)
  Eq val -> modifyToPsFormat val
  Null -> A.Null
  GreaterThan val -> single "$gt" (modifyToPsFormat val)
  GreaterThanOrEq val -> single "$gte" (modifyToPsFormat val)
  LessThan val -> single "$lt" (modifyToPsFormat val)
  LessThanOrEq val -> single "$lte" (modifyToPsFormat val)
  -- Like val -> single "$like" (modifyToPsFormat val)
  -- Not (Like val) -> single "$notLike" (modifyToPsFormat val)
  Not (In vals) -> array "$notIn" (modifyToPsFormat <$> vals)
  Not (Eq val) -> single "$ne" (modifyToPsFormat val)
  Not Null -> single "$ne" A.Null
  Not term -> single "$not" ((encodeTerm @table) key term)
  _ -> error "Error while encoding - Term not supported"

  where
    modifyToPsFormat val = snd $ (toPSJSON @be @table) (key, A.toJSON $ convertToSQLObject val)

array :: Text -> [A.Value] -> A.Value
array k vs = A.toJSON $ AKM.singleton (AKey.fromText k) vs

single :: Text -> A.Value -> A.Value
single k v = A.toJSON $ AKM.singleton (AKey.fromText k) v
