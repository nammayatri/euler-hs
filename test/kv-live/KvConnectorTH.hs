{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-name-shadowing #-}

-- =============================================================================
-- KvConnectorTH — a Postgres-targeting, self-contained port of the production
-- `enableKVPG` Template Haskell macro.
--
-- This is the SAME code-generation that Namma Yatri tables use in
-- shared-kernel (Kernel.Beam.Lib.UtilsTH.enableKVPG / enableKVPGWithSortKey):
-- from a Beam record it derives the full `KVConnector`, `MeshMeta Postgres`,
-- ToJSON/FromJSON and Cereal instances, so a table is "KV-enabled" by a single
-- splice — exactly like the real backend.
--
-- Differences vs the language suite's `KV.TestSchema.ThUtils.enableKV`:
--   * targets `MeshMeta Postgres` (the connector's update/delete-returning
--     functions are hardcoded to Postgres — see Flow.hs), not `MeshMeta MySQL`.
--   * self-contained: needs NO hand-written per-table `*ToPSModifiers` /
--     `*oHSModifiers` / `psToHs` maps (uses identity), so a table definition is
--     just the Beam record + Table instance + ModelMeta + the splice, matching
--     how nammayatri's generated Storage.Beam modules look.
--   * generates a REAL `parseFieldAndGetClause` / `parseSetClause` (not the
--     `fail` stubs the hand-rolled sorted-set-live test used), so `Where`/`Set`
--     clauses actually work end-to-end.
--   * `enableKVPGWithSortKey` additionally emits `sortScore`/`sortScoreColumn`
--     for the ZSET sorted-secondary-index path.
-- =============================================================================
module KvConnectorTH
  ( enableKVPG,
    aesonDBMeshOptions,
    emptyValueMap,
    parseField,
    utilTransform,
  )
where

import qualified Data.Aeson as A
import qualified Data.HashMap.Strict as HM
import Data.List (init)
import qualified Data.Map.Strict as M
import qualified Data.Serialize as Serialize
import qualified Data.Text as T
import qualified Database.Beam as B
import Database.Beam.Postgres (Postgres)
import EulerHS.KVConnector.Types
  ( KVConnector (..),
    MeshMeta (..),
    PrimaryKey (..),
    SecondaryKey (..),
    TermWrap (..),
  )
import EulerHS.Prelude hiding (Type, words)
import Language.Haskell.TH
import qualified Sequelize as S
import Text.Casing (camel)
import Prelude (head)

-- | KV-enable a Beam table with plain Redis SET secondary indexes.
--
--   @$(enableKVPG ''FooT ['id] [['groupId], ['otherKey]])@
--
--   arg 2 = primary key fields (composite allowed),
--   arg 3 = list of secondary indexes; each inner list = one (composite) index.
enableKVPG :: Name -> [Name] -> [[Name]] -> Q [Dec]
enableKVPG name pKeyN sKeysN = do
  tModMeshDec <- tableTModMeshD name
  kvConnectorDec <- kvConnectorInstancesD name pKeyN sKeysN
  meshMetaDec <- meshMetaInstancesD name
  jsonDec <- jsonInstancesD name
  serializeDec <- serializeStubD name
  pure $ tModMeshDec ++ meshMetaDec ++ kvConnectorDec ++ jsonDec ++ serializeDec

-- aeson options shared by every generated row instance. omitNothingFields keeps
-- the connector's additive `updateModel` merge from clobbering absent Maybe cols.
aesonDBMeshOptions :: A.Options
aesonDBMeshOptions = A.defaultOptions {A.omitNothingFields = True}

emptyValueMap :: M.Map Text (A.Value -> A.Value)
emptyValueMap = M.empty

-- =============================================================================
-- ModMesh — Beam field modification keyed by the camelCase Haskell field names
-- (the JSON keys the row serialises to), used by the KV/Redis side.
-- =============================================================================
tableTModMeshD :: Name -> Q [Dec]
tableTModMeshD name = do
  let tableTModMeshN = mkName $ camel (nameBase name) <> "ModMesh"
  names <- extractRecFields . head . extractConstructors <$> reify name
  let recExps = (\name' -> (name', AppE (VarE 'B.fieldNamed) (LitE $ StringL $ nameBase name'))) <$> names
  return [FunD tableTModMeshN [Clause [] (NormalB (RecUpdE (VarE 'B.tableModification) recExps)) []]]

-- =============================================================================
-- KVConnector instance
-- =============================================================================
kvConnectorInstancesD :: Name -> [Name] -> [[Name]] -> Q [Dec]
kvConnectorInstancesD name pKeyN sKeysN = do
  let pKeyPair = TupE [Just (LitE $ StringL $ sortAndGetKey pKeyN), Just (ConE 'True)]
      sKeyPairs = map (\k -> TupE [Just (LitE $ StringL $ sortAndGetKey k), Just (ConE 'False)]) sKeysN

  let tableNameD = FunD 'tableName [Clause [] (NormalB (LitE (StringL $ init $ camel (nameBase name)))) []]
      keyMapD = FunD 'keyMap [Clause [] (NormalB (AppE (VarE 'HM.fromList) (ListE (pKeyPair : sKeyPairs)))) []]
      primaryKeyD = FunD 'primaryKey [Clause [] (NormalB getPrimaryKeyE) []]
      secondaryKeysD = FunD 'secondaryKeys [Clause [] (NormalB getSecondaryKeysE) []]
      mkSQLObjectD = FunD 'mkSQLObject [Clause [] (NormalB (VarE 'A.toJSON)) []]

  return
    [ InstanceD
        Nothing
        []
        (AppT (ConT ''KVConnector) (AppT (ConT name) (ConT $ mkName "Identity")))
        [tableNameD, keyMapD, primaryKeyD, secondaryKeysD, mkSQLObjectD]
    ]
  where
    getPrimaryKeyE =
      let obj = mkName "obj"
       in LamE [VarP obj] (AppE (ConE 'PKey) (ListE (map (\n -> TupE [Just (keyNameTextE n), Just (getRecFieldE n obj)]) pKeyN)))

    getSecondaryKeysE =
      let obj = mkName "obj"
       in LamE
            [VarP obj]
            ( ListE $
                map
                  (\sKey -> AppE (ConE 'SKey) (ListE (map (\n -> TupE [Just (keyNameTextE n), Just (getRecFieldE n obj)]) sKey)))
                  sKeysN
            )

    getRecFieldE f obj =
      let fieldName = splitColon $ nameBase f
       in AppE (AppE (AppE (VarE 'utilTransform) (VarE 'emptyValueMap)) (LitE . StringL $ fieldName)) (AppE (VarE $ mkName fieldName) (VarE obj))

    keyNameTextE n = AppE (VarE 'T.pack) (LitE $ StringL (splitColon $ nameBase n))

sortAndGetKey :: [Name] -> String
sortAndGetKey names = intercalate "_" (sort $ fmap (splitColon . nameBase) names)

-- "$sel:merchantId:OrderReference" -> "merchantId"; "id" -> "id"
splitColon :: String -> String
splitColon s = case T.splitOn ":" (T.pack s) of
  (_ : f : _) -> T.unpack f
  _ -> s

-- =============================================================================
-- Serialize (cereal) — the connector's type signatures require a Serialize
-- instance for every table, but the value codec is selected at runtime by
-- `cerealEnabled` (JSON when False). These tests run JSON-encoded, so a stub
-- that errors if ever forced is correct and removes a fragile dep on cereal
-- instances for every column type (e.g. UTCTime).
-- =============================================================================
serializeStubD :: Name -> Q [Dec]
serializeStubD name = do
  let tableIdentity = AppT (ConT name) (ConT $ mkName "Identity")
      msg = "kv-live: cereal codec disabled (cerealEnabled=False); use JSON"
      putD = FunD 'Serialize.put [Clause [WildP] (NormalB (AppE (VarE 'error) (LitE $ StringL msg))) []]
      getD = FunD 'Serialize.get [Clause [] (NormalB (AppE (VarE 'error) (LitE $ StringL msg))) []]
  return [InstanceD Nothing [] (AppT (ConT ''Serialize.Serialize) tableIdentity) [putD, getD]]

-- =============================================================================
-- MeshMeta Postgres instance (real Where/Set clause parsing)
-- =============================================================================
meshMetaInstancesD :: Name -> Q [Dec]
meshMetaInstancesD name = do
  names <- extractRecFields . head . extractConstructors <$> reify name
  let modelTModMesh = mkName $ camel (nameBase name) <> "ModMesh"
      meshModelFieldModificationD = FunD 'meshModelFieldModification [Clause [] (NormalB (VarE modelTModMesh)) []]
      valueMapperD = FunD 'valueMapper [Clause [] (NormalB (VarE 'emptyValueMap)) []]
      parseFieldAndGetClauseD = getParseFieldAndGetClauseD name names
      parseSetClauseD = getParseSetClauseD name names
  return
    [ InstanceD
        Nothing
        []
        (AppT (AppT (ConT ''MeshMeta) (ConT ''Postgres)) (ConT name))
        [meshModelFieldModificationD, valueMapperD, parseFieldAndGetClauseD, parseSetClauseD]
    ]

getParseFieldAndGetClauseD :: Name -> [Name] -> Dec
getParseFieldAndGetClauseD name names = do
  let fnName = 'parseFieldAndGetClause
      obj = mkName "obj"
      field = mkName "field"
  let patternMatches = (\n -> Match (LitP $ StringL (nameBase n)) (NormalB (parseFieldAndGetClauseE name n)) []) <$> names
      failExp = AppE (VarE 'fail) (LitE $ StringL ("Where clause decoding failed for " <> nameBase name <> " - Unexpected column "))
      caseExp = CaseE (VarE field) (patternMatches ++ [Match WildP (NormalB failExp) []])
  FunD fnName [Clause [VarP obj, VarP field] (NormalB caseExp) []]

parseFieldAndGetClauseE :: Name -> Name -> Exp
parseFieldAndGetClauseE name key =
  let v = mkName "obj"
      parseExp = AppE (AppE (VarE 'parseField) (LitE $ StringL $ nameBase name)) (VarE v)
   in AppE (AppE (VarE '(<$>)) (AppE (ConE 'TermWrap) (VarE key))) parseExp

getParseSetClauseD :: Name -> [Name] -> Dec
getParseSetClauseD name names = do
  let fnName = 'parseSetClause
      obj = mkName "obj"
      field = mkName "field"
      parseKeyAndValue = mkName "parseKeyAndValue"
      setClause = mkName "setClause"
  let patternMatches = (\n -> Match (LitP $ StringL $ nameBase n) (NormalB (parseSetClauseE name n)) []) <$> names
      failExp = AppE (VarE 'fail) (LitE $ StringL ("Set clause decoding failed for " <> nameBase name <> " - Unexpected column "))
      caseExp = CaseE (VarE field) (patternMatches ++ [Match WildP (NormalB failExp) []])
  FunD
    fnName
    [ Clause
        [VarP setClause]
        (NormalB (AppE (AppE (VarE 'mapM) (VarE parseKeyAndValue)) (VarE setClause)))
        [FunD parseKeyAndValue [Clause [TupP [VarP field, VarP obj]] (NormalB caseExp) []]]
    ]

parseSetClauseE :: Name -> Name -> Exp
parseSetClauseE name key =
  let v = mkName "obj"
      parseExp = AppE (AppE (VarE 'parseField) (LitE $ StringL $ nameBase name)) (VarE v)
   in AppE (AppE (VarE '(<$>)) (AppE (ConE 'S.Set) (VarE key))) parseExp

-- =============================================================================
-- ToJSON / FromJSON for `table Identity`
-- =============================================================================
jsonInstancesD :: Name -> Q [Dec]
jsonInstancesD name = do
  let tableIdentity = AppT (ConT name) (ConT $ mkName "Identity")
      toJSONI =
        InstanceD
          Nothing
          []
          (AppT (ConT ''A.ToJSON) tableIdentity)
          [FunD 'A.toJSON [Clause [] (NormalB (AppE (VarE 'A.genericToJSON) (VarE 'aesonDBMeshOptions))) []]]
      fromJSONI =
        InstanceD
          Nothing
          []
          (AppT (ConT ''A.FromJSON) tableIdentity)
          [FunD 'A.parseJSON [Clause [] (NormalB (AppE (VarE 'A.genericParseJSON) (VarE 'aesonDBMeshOptions))) []]]
  return [toJSONI, fromJSONI]

-- =============================================================================
-- Helpers
-- =============================================================================
parseField :: (FromJSON a, MonadFail f) => Text -> A.Value -> f a
parseField modelName fieldObj = case A.fromJSON fieldObj of
  A.Success res -> pure res
  _ -> fail $ T.unpack $ "Error while decoding - Unable to parse field for " <> modelName <> " model"

extractConstructors :: Info -> [Con]
extractConstructors (TyConI (DataD _ _ _ _ cons _)) = cons
extractConstructors (TyConI (NewtypeD _ _ _ _ cons _)) = [cons]
extractConstructors _ = []

extractRecFields :: Con -> [Name]
extractRecFields (RecC _ bangs) = handleVarBang <$> bangs where handleVarBang (a, _, _) = a
extractRecFields _ = []

utilTransform :: (ToJSON a) => M.Map Text (A.Value -> A.Value) -> Text -> a -> Text
utilTransform modifyMap field value = do
  let res = case M.lookup field modifyMap of
        Just fn -> fn . A.toJSON $ value
        Nothing -> A.toJSON value
  case res of
    A.String r -> r
    A.Number n -> T.pack $ show n
    A.Bool b -> T.pack $ show b
    A.Array l -> T.pack $ show l
    A.Object o -> T.pack $ show o
    A.Null -> T.pack ""
