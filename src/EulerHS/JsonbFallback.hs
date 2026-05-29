{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE DerivingStrategies   #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE GADTs                #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RankNTypes           #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Schema-tolerant Postgres reads: on @42703 column does not exist@ retry
-- via @SELECT to_jsonb(t.*)@ and decode through the table's @FromJSON@.
-- Wired into 'EulerHS.CachedSqlDBQuery.findAllSql' / 'findOneSql' via the
-- 'TryJsonbFallback' typeclass — external callers need no source changes.
module EulerHS.JsonbFallback
  ( TryJsonbFallback (..)
  , findAllSqlJsonb
  , findOneSqlJsonb
  , fireFallbackHook
  , renderWhere
  , JsonbFallbackError (..)
  ) where

import           Control.Exception                    (SomeException, try)
import qualified Data.Aeson                           as A
import qualified Data.Aeson.Key                       as AK
import qualified Data.Aeson.KeyMap                    as AKM
import           Data.Maybe                           (listToMaybe)
import qualified Data.Pool                            as DP
import qualified Data.Text                            as T
import qualified Data.Text.Encoding                   as TE
import qualified Database.Beam.Postgres               as BP
import           Database.Beam.Schema.Tables          (TableField (..),
                                                       TableSettings,
                                                       dbTableSettings)
import qualified Database.PostgreSQL.Simple           as PGS
import           Database.PostgreSQL.Simple.Types     (Query (..))
import           EulerHS.Extra.Language               (getOrInitSqlConn)
import qualified EulerHS.Framework.Language           as L
import qualified EulerHS.KVConnector.Metrics          as KVM
import           EulerHS.Prelude                      hiding (try)
import           EulerHS.SqlDB.Types                  (DBConfig,
                                                       NativeSqlPool (..),
                                                       SqlConn, bemToNative)
import           Sequelize                            (Clause (..), Column,
                                                       Model, ModelMeta,
                                                       Term (..), Where,
                                                       columnize, fromColumnar',
                                                       modelSchemaName,
                                                       modelTableEntityDescriptor,
                                                       modelTableName)
import           Sequelize.SQLObject                  (SQLObject (..),
                                                       ToSQLObject,
                                                       convertToSQLObject)
import qualified Text.Casing                          as Casing

data JsonbFallbackError
  = JfeConnectionInitFailed     Text
  | JfeUnexpectedConnectionKind Text
  | JfePostgresError            Text
  | JfeJsonDecodeError          Text
  | JfeUnsupportedWhereTerm     Text
  deriving stock    (Show, Generic)
  deriving anyclass (A.ToJSON)

-- | Log the original Postgres error and bump @kv_jsonb_fallback_counter@
-- BEFORE the retry runs. Gated by @KV_METRIC_ENABLED@ via the same
-- 'KVMetricCfg' / 'L.getOption' dispatch as 'incrementRedisCallMetric'.
fireFallbackHook
  :: forall table m
   . (HasCallStack, L.MonadFlow m, ModelMeta table)
  => Text -> m ()
fireFallbackHook errText = do
  let schema  = fromMaybe "" (modelSchemaName @table)
      tbl     = modelTableName  @table
      payload = A.object
        [ "schema" A..= schema
        , "table"  A..= tbl
        , "error"  A..= errText
        , "action" A..= ("retrying_via_to_jsonb" :: Text)
        ]
  L.logErrorV ("JsonbFallbackTriggered" :: Text) payload
  KVM.incrementJsonbFallbackMetric schema tbl errText

--------------------------------------------------------------------------------
-- WHERE rendering: Sequelize 'Where' → raw SQL fragment.
--
-- Sequelize's @valueToText = T.pack . show@ wraps text literals in escaped
-- quotes (@"M1"@ → @"\"M1\""@); 'unShowText' strips that so the resulting
-- SQL matches what beam would have produced.
--------------------------------------------------------------------------------

columnName
  :: forall table value. (Model BP.Postgres table)
  => Column table value -> Text
columnName col =
  let settings :: TableSettings table
      settings = dbTableSettings (modelTableEntityDescriptor @table @BP.Postgres)
      field    = fromColumnar' (col (columnize settings)) :: TableField table value
   in _fieldName field

unShowText :: Text -> Text
unShowText t
  | T.length t >= 2, T.head t == '"', T.last t == '"' =
      T.replace "\\\"" "\"" $ T.init (T.tail t)
  | otherwise = t

quoteSqlString, quoteIdent :: Text -> Text
quoteSqlString t = "'"  <> T.replace "'"  "''"   t <> "'"
quoteIdent     t = "\"" <> T.replace "\"" "\"\"" t <> "\""

sqlLit, sqlInList :: SQLObject a -> Text
sqlLit = \case
  SQLObjectValue v  -> quoteSqlString (unShowText v)
  SQLObjectList xs  -> "ARRAY[" <> T.intercalate "," (map sqlLit xs) <> "]"
sqlInList = \case
  SQLObjectValue v  -> "(" <> quoteSqlString (unShowText v) <> ")"
  SQLObjectList xs  -> "(" <> T.intercalate "," (map sqlLit xs) <> ")"

renderWhere
  :: forall table. (Model BP.Postgres table)
  => Where BP.Postgres table -> Either JsonbFallbackError Text
renderWhere = \case
  []       -> Right "TRUE"
  [c]      -> renderClause c
  cs       -> renderClause (And cs)
  where
    renderClause :: Clause BP.Postgres table -> Either JsonbFallbackError Text
    renderClause = \case
      And cs    -> joinClauses " AND " cs
      Or  cs    -> joinClauses " OR "  cs
      Is col tm -> renderTerm (columnName @table col) tm

    joinClauses :: Text -> [Clause BP.Postgres table] -> Either JsonbFallbackError Text
    joinClauses sep cs = do
      parts <- traverse renderClause cs
      pure $ case parts of
        []  -> "TRUE"
        [x] -> x
        xs  -> "(" <> T.intercalate (")" <> sep <> "(") xs <> ")"

    renderTerm
      :: forall v. (ToSQLObject v)
      => Text -> Term BP.Postgres v -> Either JsonbFallbackError Text
    renderTerm c = \case
      Eq v              -> Right $ quoteIdent c <> " = "    <> sqlLit    (convertToSQLObject v)
      In vs             -> Right $ quoteIdent c <> " IN "   <> sqlInList (convertToSQLObject vs)
      Null              -> Right $ quoteIdent c <> " IS NULL"
      GreaterThan v     -> Right $ quoteIdent c <> " > "    <> sqlLit (convertToSQLObject v)
      GreaterThanOrEq v -> Right $ quoteIdent c <> " >= "   <> sqlLit (convertToSQLObject v)
      LessThan v        -> Right $ quoteIdent c <> " < "    <> sqlLit (convertToSQLObject v)
      LessThanOrEq v    -> Right $ quoteIdent c <> " <= "   <> sqlLit (convertToSQLObject v)
      Like t            -> Right $ quoteIdent c <> " LIKE " <> quoteSqlString t
      Not inner -> case inner of
        Eq v  -> Right $ quoteIdent c <> " <> "     <> sqlLit    (convertToSQLObject v)
        In vs -> Right $ quoteIdent c <> " NOT IN " <> sqlInList (convertToSQLObject vs)
        Null  -> Right $ quoteIdent c <> " IS NOT NULL"
        other -> (\inner' -> "NOT (" <> inner' <> ")") <$> renderTerm c other

--------------------------------------------------------------------------------
-- Execute SELECT to_jsonb(t.*) and decode rows
--------------------------------------------------------------------------------

-- | Normalise a row JSON value for downstream @FromJSON@. All transforms
-- apply ONLY to top-level column values — nested objects / arrays are user
-- payload (JSONB / @text[]@) and must stay byte-identical.
--
-- 1. Rewrite top-level keys @snake_case → camelCase@ so the
--    @mkTableInstances@-generated @genericParseJSON defaultOptions@ instances
--    (which expect Haskell field names) decode unchanged.
-- 2. Strip the Postgres bytea text-format @\\x@ prefix from string values
--    (@"\\xdeadbeef" → "deadbeef"@) so @DbHash@-style @decodeHex@ accepts them.
-- 3. Unwrap @TEXT@ columns that hold a serialised JSON object / array (the
--    @mkBeamInstancesForJSON@ shape) — @to_jsonb@ emits them as JSON strings
--    @\"{...}\"@, the type's auto-derived FromJSON expects the structured
--    Value. We parse and substitute only when the inner parse yields an
--    Object or Array (scalar-looking strings are left untouched).
normaliseRow :: A.Value -> A.Value
normaliseRow = \case
  A.Object o -> A.Object $ AKM.fromList
    [ (AK.fromText (T.pack (Casing.camel (T.unpack (AK.toText k)))), normaliseValue v)
    | (k, v) <- AKM.toList o ]
  other      -> other

normaliseValue :: A.Value -> A.Value
normaliseValue = unwrapJsonText . unbyteaEncode

unbyteaEncode :: A.Value -> A.Value
unbyteaEncode = \case
  A.String t
    | Just rest <- T.stripPrefix "\\x" t
    , not (T.null rest)
    , T.all isHexChar rest -> A.String rest
  other -> other
  where
    isHexChar c = (c >= '0' && c <= '9')
               || (c >= 'a' && c <= 'f')
               || (c >= 'A' && c <= 'F')

-- | Promote @TEXT@-stored JSON to its parsed Value, but only when the string
-- starts with @{@ or @[@ and parses to a structured value. Plain strings
-- (even those starting with other JSON-prefix characters) are preserved.
unwrapJsonText :: A.Value -> A.Value
unwrapJsonText = \case
  A.String t
    | not (T.null t)
    , T.head t == '{' || T.head t == '['
    , Right parsed <- A.eitherDecodeStrict (TE.encodeUtf8 t)
    , structured parsed
    -> parsed
  other -> other
  where
    structured (A.Object _) = True
    structured (A.Array  _) = True
    structured _            = False

runJsonbSelect
  :: forall table m
   . (HasCallStack, L.MonadFlow m, A.FromJSON (table Identity))
  => DBConfig BP.Pg -> Text
  -> m (Either JsonbFallbackError [table Identity])
runJsonbSelect dbConf sql = do
  eConn <- getOrInitSqlConn dbConf
  case eConn of
    Left e -> pure $ Left $ JfeConnectionInitFailed (T.pack (show e))
    Right (conn :: SqlConn BP.Pg) -> case bemToNative conn of
      NativePGPool pool -> do
        ePg <- L.runIO $ try $ DP.withResource pool $ \pgConn ->
          PGS.query_ pgConn (Query (TE.encodeUtf8 sql)) :: IO [PGS.Only A.Value]
        case ePg of
          Left  (e :: SomeException) -> pure $ Left $ JfePostgresError (T.pack (show e))
          Right onlys                -> pure $ decodeRows (map PGS.fromOnly onlys) []
      _ -> pure $ Left $ JfeUnexpectedConnectionKind "expected NativePGPool"
  where
    decodeRows :: [A.Value] -> [table Identity] -> Either JsonbFallbackError [table Identity]
    decodeRows []     acc = Right (reverse acc)
    decodeRows (v:vs) acc = case A.fromJSON (normaliseRow v) of
      A.Success r -> decodeRows vs (r : acc)
      A.Error err -> Left (JfeJsonDecodeError (T.pack err))

qualifiedTableName :: forall table. ModelMeta table => Text
qualifiedTableName = case modelSchemaName @table of
  Just s  -> quoteIdent s <> "." <> quoteIdent (modelTableName @table)
  Nothing -> quoteIdent (modelTableName @table)

findAllSqlJsonb
  :: forall table m
   . (HasCallStack, L.MonadFlow m, Model BP.Postgres table, A.FromJSON (table Identity))
  => DBConfig BP.Pg -> Where BP.Postgres table
  -> m (Either JsonbFallbackError [table Identity])
findAllSqlJsonb dbConf w = case renderWhere @table w of
  Left e  -> pure (Left e)
  Right s -> runJsonbSelect @table dbConf $
    "SELECT to_jsonb(t.*) FROM " <> qualifiedTableName @table <> " t WHERE " <> s

findOneSqlJsonb
  :: forall table m
   . (HasCallStack, L.MonadFlow m, Model BP.Postgres table, A.FromJSON (table Identity))
  => DBConfig BP.Pg -> Where BP.Postgres table
  -> m (Either JsonbFallbackError (Maybe (table Identity)))
findOneSqlJsonb dbConf w = case renderWhere @table w of
  Left e  -> pure (Left e)
  Right s -> fmap listToMaybe <$> runJsonbSelect @table dbConf
    ("SELECT to_jsonb(t.*) FROM " <> qualifiedTableName @table <> " t WHERE " <> s <> " LIMIT 1")

--------------------------------------------------------------------------------
-- Backend-polymorphic dispatch used by CachedSqlDBQuery.{findAllSql,findOneSql}.
-- OVERLAPPABLE default returns Nothing (any backend); OVERLAPPING Pg+Postgres
-- specialization runs the to_jsonb fallback after firing the metric/log.
--------------------------------------------------------------------------------

class TryJsonbFallback beM be table where
  tryJsonbFallback
    :: (HasCallStack, L.MonadFlow m, Model be table, ModelMeta table)
    => DBConfig beM -> Where be table -> Text
    -> m (Maybe [table Identity])

instance {-# OVERLAPPABLE #-} TryJsonbFallback beM be table where
  tryJsonbFallback _ _ _ = pure Nothing

instance {-# OVERLAPPING #-}
         (A.FromJSON (table Identity))
         => TryJsonbFallback BP.Pg BP.Postgres table where
  tryJsonbFallback dbConf w origErr = do
    fireFallbackHook @table origErr
    eRes <- findAllSqlJsonb @table dbConf w
    pure $ case eRes of
      Right rs -> Just rs
      Left  _  -> Nothing
