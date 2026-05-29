{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Schema-tolerant Postgres reads: on @42703 column does not exist@ retry
-- via @SELECT to_jsonb(t.*)@ and decode through the table's @FromJSON@.
-- Wired into 'EulerHS.CachedSqlDBQuery.findAllSql' / 'findOneSql' via the
-- 'TryJsonbFallback' typeclass — external callers need no source changes.
module EulerHS.JsonbFallback
  ( TryJsonbFallback (..),
    GFieldNames,
    findAllSqlJsonb,
    findOneSqlJsonb,
    fireFallbackHook,
    renderWhere,
    JsonbFallbackError (..),
  )
where

import Control.Exception (SomeException, try)
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as AK
import qualified Data.Aeson.KeyMap as AKM
import qualified Data.HashMap.Strict as HM
import Data.Maybe (listToMaybe)
import qualified Data.Pool as DP
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Database.Beam.Postgres as BP
import Database.Beam.Schema.Tables
  ( Columnar' (..),
    TableField (..),
    TableSettings,
    allBeamValues,
    dbTableSettings,
  )
import qualified Database.PostgreSQL.Simple as PGS
import Database.PostgreSQL.Simple.Types (Query (..))
import EulerHS.Extra.Language (getOrInitSqlConn)
import qualified EulerHS.Framework.Language as L
import qualified EulerHS.KVConnector.Metrics as KVM
import EulerHS.Prelude hiding (try)
import EulerHS.SqlDB.Types
  ( DBConfig,
    NativeSqlPool (..),
    SqlConn,
    bemToNative,
  )
import qualified GHC.Generics as G
import Sequelize
  ( Clause (..),
    Column,
    Model,
    ModelMeta,
    Term (..),
    Where,
    columnize,
    fromColumnar',
    modelSchemaName,
    modelTableEntityDescriptor,
    modelTableName,
  )
import Sequelize.SQLObject
  ( SQLObject (..),
    ToSQLObject,
    convertToSQLObject,
  )
import qualified Text.Casing as Casing

data JsonbFallbackError
  = JfeConnectionInitFailed Text
  | JfeUnexpectedConnectionKind Text
  | JfePostgresError Text
  | JfeJsonDecodeError Text
  | JfeUnsupportedWhereTerm Text
  deriving stock (Show, Generic)
  deriving anyclass (A.ToJSON)

-- | Emit a structured error log and bump @kv_jsonb_fallback_counter@ BEFORE
-- the retry runs. The metric is ungated (rare, high-signal event).
fireFallbackHook ::
  forall table m.
  (HasCallStack, L.MonadFlow m, ModelMeta table) =>
  Text ->
  m ()
fireFallbackHook errText = do
  let schema = fromMaybe "" (modelSchemaName @table)
      tbl = modelTableName @table
      payload =
        A.object
          [ "schema" A..= schema,
            "table" A..= tbl,
            "error" A..= errText,
            "action" A..= ("retrying_via_to_jsonb" :: Text)
          ]
  L.logErrorV ("JsonbFallbackTriggered" :: Text) payload
  KVM.incrementJsonbFallbackMetric schema tbl errText

-- Sequelize's @valueToText = T.pack . show@ wraps text literals in escaped
-- quotes (@"M1"@ → @"\"M1\""@); 'unShowText' strips that so the SQL matches
-- what beam would have produced.

columnName ::
  forall table value.
  (Model BP.Postgres table) =>
  Column table value ->
  Text
columnName col =
  let settings :: TableSettings table
      settings = dbTableSettings (modelTableEntityDescriptor @table @BP.Postgres)
      field = fromColumnar' (col (columnize settings)) :: TableField table value
   in _fieldName field

unShowText :: Text -> Text
unShowText t
  | T.length t >= 2,
    T.head t == '"',
    T.last t == '"' =
    T.replace "\\\"" "\"" $ T.init (T.tail t)
  | otherwise = t

quoteSqlString, quoteIdent :: Text -> Text
quoteSqlString t = "'" <> T.replace "'" "''" t <> "'"
quoteIdent t = "\"" <> T.replace "\"" "\"\"" t <> "\""

sqlLit, sqlInList :: SQLObject a -> Text
sqlLit = \case
  SQLObjectValue v -> quoteSqlString (unShowText v)
  SQLObjectList xs -> "ARRAY[" <> T.intercalate "," (map sqlLit xs) <> "]"
sqlInList = \case
  SQLObjectValue v -> "(" <> quoteSqlString (unShowText v) <> ")"
  SQLObjectList xs -> "(" <> T.intercalate "," (map sqlLit xs) <> ")"

renderWhere ::
  forall table.
  (Model BP.Postgres table) =>
  Where BP.Postgres table ->
  Either JsonbFallbackError Text
renderWhere = \case
  [] -> Right "TRUE"
  [c] -> renderClause c
  cs -> renderClause (And cs)
  where
    renderClause :: Clause BP.Postgres table -> Either JsonbFallbackError Text
    renderClause = \case
      And cs -> joinClauses " AND " cs
      Or cs -> joinClauses " OR " cs
      Is col tm -> renderTerm (columnName @table col) tm

    joinClauses :: Text -> [Clause BP.Postgres table] -> Either JsonbFallbackError Text
    joinClauses sep cs = do
      parts <- traverse renderClause cs
      pure $ case parts of
        [] -> "TRUE"
        [x] -> x
        xs -> "(" <> T.intercalate (")" <> sep <> "(") xs <> ")"

    renderTerm ::
      forall v.
      (ToSQLObject v) =>
      Text ->
      Term BP.Postgres v ->
      Either JsonbFallbackError Text
    renderTerm c = \case
      Eq v -> Right $ quoteIdent c <> " = " <> sqlLit (convertToSQLObject v)
      In vs -> Right $ quoteIdent c <> " IN " <> sqlInList (convertToSQLObject vs)
      Null -> Right $ quoteIdent c <> " IS NULL"
      GreaterThan v -> Right $ quoteIdent c <> " > " <> sqlLit (convertToSQLObject v)
      GreaterThanOrEq v -> Right $ quoteIdent c <> " >= " <> sqlLit (convertToSQLObject v)
      LessThan v -> Right $ quoteIdent c <> " < " <> sqlLit (convertToSQLObject v)
      LessThanOrEq v -> Right $ quoteIdent c <> " <= " <> sqlLit (convertToSQLObject v)
      Like t -> Right $ quoteIdent c <> " LIKE " <> quoteSqlString t
      Not inner -> case inner of
        Eq v -> Right $ quoteIdent c <> " <> " <> sqlLit (convertToSQLObject v)
        In vs -> Right $ quoteIdent c <> " NOT IN " <> sqlInList (convertToSQLObject vs)
        Null -> Right $ quoteIdent c <> " IS NOT NULL"
        other -> (\inner' -> "NOT (" <> inner' <> ")") <$> renderTerm c other

-- | Walk a record's 'G.Rep' to extract Haskell record-selector names in
-- declaration order. Used on @table Identity@ to get the camelCase field
-- names that @genericParseJSON defaultOptions@ expects.
class GFieldNames (f :: Type -> Type) where
  gFieldNames :: Proxy f -> [Text]

instance GFieldNames G.U1 where gFieldNames _ = []

instance G.Selector s => GFieldNames (G.M1 G.S s a) where gFieldNames _ = [T.pack (G.selName (undefined :: G.M1 G.S s a x))]

instance (GFieldNames f, GFieldNames g) => GFieldNames (f G.:*: g) where gFieldNames _ = gFieldNames (Proxy @f) <> gFieldNames (Proxy @g)

instance GFieldNames f => GFieldNames (G.M1 G.D s f) where gFieldNames _ = gFieldNames (Proxy @f)

instance GFieldNames f => GFieldNames (G.M1 G.C s f) where gFieldNames _ = gFieldNames (Proxy @f)

-- | @<db-column-name> → <haskell-record-selector-name>@. DB names from
-- 'modelFieldModification' (covers @mkTableInstancesWithTModifier@ overrides),
-- Haskell names from 'GHC.Generics'; paired in beam's structural order.
fieldNameMap ::
  forall table.
  (Model BP.Postgres table, G.Generic (table Identity), GFieldNames (G.Rep (table Identity))) =>
  HM.HashMap Text Text
fieldNameMap =
  let modSettings :: TableSettings table
      modSettings = dbTableSettings (modelTableEntityDescriptor @table @BP.Postgres)
      pick :: forall a. Columnar' (TableField table) a -> Text
      pick (Columnar' f) = _fieldName f
   in HM.fromList $
        zip
          (allBeamValues pick modSettings)
          (gFieldNames (Proxy @(G.Rep (table Identity))))

-- | Normalise a row Value for the table's auto-derived @FromJSON@. All
-- transforms are top-level only — nested objects / arrays (JSONB columns,
-- text[] arrays) stay byte-identical.
--
-- * Key: DB column name → Haskell selector name via 'fieldNameMap'; falls
--   back to snake↔camel for any column not in the map.
-- * Value: 'unbyteaEncode' strips bytea's @\\x@ prefix; 'unwrapJsonText'
--   promotes TEXT-stored JSON objects/arrays so @mkBeamInstancesForJSON@
--   FromJSON instances receive the structured Value, not a JSON string.
normaliseRow ::
  forall table.
  (Model BP.Postgres table, G.Generic (table Identity), GFieldNames (G.Rep (table Identity))) =>
  A.Value ->
  A.Value
normaliseRow = \case
  A.Object o ->
    A.Object $
      AKM.fromList
        [ (renameKey (AK.toText k), normaliseValue v)
          | (k, v) <- AKM.toList o
        ]
  other -> other
  where
    nameMap = fieldNameMap @table
    renameKey dbCol =
      AK.fromText $
        HM.lookupDefault (T.pack (Casing.camel (T.unpack dbCol))) dbCol nameMap

normaliseValue :: A.Value -> A.Value
normaliseValue = unwrapJsonText . unbyteaEncode

unbyteaEncode :: A.Value -> A.Value
unbyteaEncode = \case
  A.String t
    | Just rest <- T.stripPrefix "\\x" t,
      not (T.null rest),
      T.all isHexChar rest ->
      A.String rest
  other -> other
  where
    isHexChar c =
      (c >= '0' && c <= '9')
        || (c >= 'a' && c <= 'f')
        || (c >= 'A' && c <= 'F')

unwrapJsonText :: A.Value -> A.Value
unwrapJsonText = \case
  A.String t
    | not (T.null t),
      T.head t == '{' || T.head t == '[',
      Right parsed <- A.eitherDecodeStrict (TE.encodeUtf8 t),
      structured parsed ->
      parsed
  other -> other
  where
    structured (A.Object _) = True
    structured (A.Array _) = True
    structured _ = False

type JsonbDecodeable table =
  ( Model BP.Postgres table,
    A.FromJSON (table Identity),
    G.Generic (table Identity),
    GFieldNames (G.Rep (table Identity))
  )

runJsonbSelect ::
  forall table m.
  (HasCallStack, L.MonadFlow m, JsonbDecodeable table) =>
  DBConfig BP.Pg ->
  Text ->
  m (Either JsonbFallbackError [table Identity])
runJsonbSelect dbConf sql = do
  eConn <- getOrInitSqlConn dbConf
  case eConn of
    Left e -> pure $ Left $ JfeConnectionInitFailed (T.pack (show e))
    Right (conn :: SqlConn BP.Pg) -> case bemToNative conn of
      NativePGPool pool -> do
        ePg <- L.runIO $
          try $
            DP.withResource pool $ \pgConn ->
              PGS.query_ pgConn (Query (TE.encodeUtf8 sql)) :: IO [PGS.Only A.Value]
        case ePg of
          Left (e :: SomeException) -> pure $ Left $ JfePostgresError (T.pack (show e))
          Right onlys -> pure $ decodeRows (map PGS.fromOnly onlys) []
      _ -> pure $ Left $ JfeUnexpectedConnectionKind "expected NativePGPool"
  where
    decodeRows :: [A.Value] -> [table Identity] -> Either JsonbFallbackError [table Identity]
    decodeRows [] acc = Right (reverse acc)
    decodeRows (v : vs) acc = case A.fromJSON (normaliseRow @table v) of
      A.Success r -> decodeRows vs (r : acc)
      A.Error err -> Left (JfeJsonDecodeError (T.pack err))

qualifiedTableName :: forall table. ModelMeta table => Text
qualifiedTableName = case modelSchemaName @table of
  Just s -> quoteIdent s <> "." <> quoteIdent (modelTableName @table)
  Nothing -> quoteIdent (modelTableName @table)

findAllSqlJsonb ::
  forall table m.
  (HasCallStack, L.MonadFlow m, JsonbDecodeable table) =>
  DBConfig BP.Pg ->
  Where BP.Postgres table ->
  m (Either JsonbFallbackError [table Identity])
findAllSqlJsonb dbConf w = case renderWhere @table w of
  Left e -> pure (Left e)
  Right s ->
    runJsonbSelect @table dbConf $
      "SELECT to_jsonb(t.*) FROM " <> qualifiedTableName @table <> " t WHERE " <> s

findOneSqlJsonb ::
  forall table m.
  (HasCallStack, L.MonadFlow m, JsonbDecodeable table) =>
  DBConfig BP.Pg ->
  Where BP.Postgres table ->
  m (Either JsonbFallbackError (Maybe (table Identity)))
findOneSqlJsonb dbConf w = case renderWhere @table w of
  Left e -> pure (Left e)
  Right s ->
    fmap listToMaybe
      <$> runJsonbSelect @table
        dbConf
        ("SELECT to_jsonb(t.*) FROM " <> qualifiedTableName @table <> " t WHERE " <> s <> " LIMIT 1")

-- | Backend-polymorphic dispatch used inside @CachedSqlDBQuery.findAllSql@ /
-- @findOneSql@. OVERLAPPABLE default = no-op (any backend);
-- OVERLAPPING Pg+Postgres+FromJSON = runs the to_jsonb fallback.
class TryJsonbFallback beM be table where
  tryJsonbFallback ::
    (HasCallStack, L.MonadFlow m, Model be table, ModelMeta table) =>
    DBConfig beM ->
    Where be table ->
    Text ->
    m (Maybe [table Identity])

instance {-# OVERLAPPABLE #-} TryJsonbFallback beM be table where
  tryJsonbFallback _ _ _ = pure Nothing

instance
  {-# OVERLAPPING #-}
  ( A.FromJSON (table Identity),
    G.Generic (table Identity),
    GFieldNames (G.Rep (table Identity))
  ) =>
  TryJsonbFallback BP.Pg BP.Postgres table
  where
  tryJsonbFallback dbConf w origErr = do
    fireFallbackHook @table origErr
    eRes <- findAllSqlJsonb @table dbConf w
    pure $ case eRes of
      Right rs -> Just rs
      Left _ -> Nothing
