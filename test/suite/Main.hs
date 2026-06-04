{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- =============================================================================
-- `tests` — the single entry point for the whole euler-hs test matrix.
--
--     cabal run tests
--
-- It is self-managing: it checks Postgres + Redis are up (and tells you exactly
-- how to start them if not), auto-creates the `euler_sorted_test` database, and
-- each suite cleans its own Postgres rows and Redis keys. You run one command.
--
-- What it runs (with per-suite logs + a final PASS/FAIL summary):
--   • kv-live              — every EulerHS.KVConnector.Flow function, live PG+Redis
--   • encoding/compression — value codec round-trips
--   • recache flags        — IS_CACHING_DB_FIND_ENABLED / IS_RECACHING_ENABLED
--   • jsonb-live           — jsonb fallback (skips if the atlas DB is absent)
--   • db                   — SQLite query-builder + pgexercises clubdata (hspec)
--   • extra                — aeson option deriving (hspec)
--
-- Plain `cabal build` builds ONLY the library — this is a test-suite, so it is
-- built solely on demand by `cabal run tests` / `cabal test`.
-- =============================================================================
module Main (main) where

import Control.Exception (SomeException, try)
import Control.Monad (unless, void)
import Data.ByteString (ByteString)
import Data.Text (Text)
import qualified Database.PostgreSQL.Simple as PGS
import EncodingTests (runEncodingTests)
import qualified EulerHS.Interpreters as I
import qualified EulerHS.Language as L
import qualified EulerHS.Runtime as R
import qualified EulerHS.Types as ET
import JsonbLiveTests (runJsonbTests)
import KvLiveTests (runKvLiveTests, runRecacheTests)
import qualified Options
import qualified SQLDB.Tests.QueryExamplesSpec as QueryExamples
import qualified SQLDB.Tests.SQLiteDBSpec as SQLiteDB
import System.Environment (getArgs, getEnvironment, getExecutablePath)
import System.Exit (ExitCode (..), die, exitFailure, exitSuccess, exitWith)
import System.IO (BufferMode (..), hSetBuffering, stdout)
import System.Process (CreateProcess (..), StdStream (Inherit), createProcess, proc, waitForProcess)
import Test.Hspec.Runner (Summary (..), hspecResult)
import Prelude

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  args <- getArgs
  case args of
    -- Child: recache flags are per-process CAFs, set in this child's env.
    ("--recache-phase" : _) -> do
      ok <- runRecacheTests
      exitWith (if ok then ExitSuccess else ExitFailure 1)
    -- Child: KV_METRIC_ENABLED is a per-process CAF the jsonb suite needs set.
    ("--jsonb-phase" : _) -> do
      ok <- runJsonbTests
      exitWith (if ok then ExitSuccess else ExitFailure 1)
    _ -> orchestrate

orchestrate :: IO ()
orchestrate = do
  putStrLn "╔══════════════════════════════════════════════════════════╗"
  putStrLn "║  euler-hs test matrix  (cabal run tests)                 ║"
  putStrLn "╚══════════════════════════════════════════════════════════╝"
  preflight

  -- 1) KV connector full surface (this process, default phase) -------------
  kvOk <- runKvLiveTests

  -- 1b) value codec: JSON/CBOR/DEAD headers + zstd compression -------------
  encOk <- runEncodingTests

  self <- getExecutablePath
  baseEnv <- getEnvironment

  -- 2b) Recache flags in a child process (CAF env: caching-db-find + recaching) --
  putStrLn "\n>>> launching recache-flags phase (IS_CACHING_DB_FIND_ENABLED + IS_RECACHING_ENABLED)"
  let recacheEnv =
        [("IS_CACHING_DB_FIND_ENABLED", "True"), ("IS_RECACHING_ENABLED", "True")]
          <> filter ((`notElem` ["IS_CACHING_DB_FIND_ENABLED", "IS_RECACHING_ENABLED"]) . fst) baseEnv
  (_, _, _, rph) <-
    createProcess (proc self ["--recache-phase"]) {env = Just recacheEnv, std_out = Inherit, std_err = Inherit}
  recacheCode <- waitForProcess rph
  let recacheOk = recacheCode == ExitSuccess

  -- 2c) jsonb-live in a child process (KV_METRIC_ENABLED CAF). Self-skips if the
  -- atlas_driver_offer_bpp_v1 database isn't present.
  putStrLn "\n>>> launching jsonb-live phase (KV_METRIC_ENABLED=True; skips if atlas DB absent)"
  let jsonbEnv = ("KV_METRIC_ENABLED", "True") : filter ((/= "KV_METRIC_ENABLED") . fst) baseEnv
  (_, _, _, jph) <-
    createProcess (proc self ["--jsonb-phase"]) {env = Just jsonbEnv, std_out = Inherit, std_err = Inherit}
  jsonbCode <- waitForProcess jph
  let jsonbOk = jsonbCode == ExitSuccess

  -- 3) Pure hspec suites ---------------------------------------------------
  putStrLn "\n############################################################"
  putStrLn "#  extra — aeson option deriving (hspec)"
  putStrLn "############################################################"
  exSum <- hspecResult Options.spec

  putStrLn "\n############################################################"
  putStrLn "#  db — SQLite query-builder + pgexercises clubdata (hspec)"
  putStrLn "############################################################"
  dbSum <- hspecResult (SQLiteDB.spec >> QueryExamples.spec)

  -- 4) Summary -------------------------------------------------------------
  let rows =
        [ ("kv-live (full KV connector surface)", kvOk),
          ("encoding + compression (JSON/CBOR/DEAD/zstd)", encOk)
        ]
          <> [ ("recache flags (caching-db-find + recaching)", recacheOk),
               ("jsonb-live (skipped if atlas DB absent)", jsonbOk),
               ("extra (hspec)", summaryFailures exSum == 0),
               ("db / SQLite (hspec)", summaryFailures dbSum == 0)
             ]
  putStrLn "\n╔══════════════════════════════════════════════════════════╗"
  putStrLn "║  SUMMARY                                                 ║"
  putStrLn "╚══════════════════════════════════════════════════════════╝"
  mapM_ (\(n, ok) -> putStrLn $ "  " <> (if ok then "✅ PASS  " else "❌ FAIL  ") <> n) rows
  if all snd rows
    then putStrLn "\n✅ ALL TESTS GREEN" >> exitSuccess
    else putStrLn "\n❌ FAILURES ABOVE — see logs" >> exitFailure

-- =============================================================================
-- Preflight: fail fast with actionable instructions if infra is down.
-- =============================================================================

preflight :: IO ()
preflight = do
  putStrLn "── preflight: Postgres + Redis ──────────────────────────────"

  -- Postgres server reachable? (connect to the default `postgres` db)
  ePg <-
    try (PGS.connectPostgreSQL "host=localhost port=5432 user=postgres dbname=postgres") ::
      IO (Either SomeException PGS.Connection)
  case ePg of
    Left _ ->
      die $
        unlines
          [ "",
            "❌ Postgres is not reachable on localhost:5432 (user=postgres).",
            "   Start it, then re-run `cabal run tests`. For example:",
            "       brew services start postgresql@14        # macOS / Homebrew",
            "       # or:  pg_ctl -D /opt/homebrew/var/postgresql@14 start",
            "   (a `postgres` superuser role with no password is expected on localhost)"
          ]
    Right c -> do
      ns <-
        PGS.query_ c "SELECT count(*) FROM pg_database WHERE datname = 'euler_sorted_test'" ::
          IO [PGS.Only Int]
      case ns of
        [PGS.Only 0] -> do
          putStrLn "   • database euler_sorted_test missing — creating it"
          void $ PGS.execute_ c "CREATE DATABASE euler_sorted_test"
        _ -> putStrLn "   • database euler_sorted_test present"
      PGS.close c

  -- Redis primary (6379) + secondary (6380) reachable?
  p <- redisUp redisCfg "KVRedis"
  unless p $ die (redisDownMsg 6379)
  s <- redisUp redisCfgSecondary "KVRedisSecondary"
  unless s $ die (redisDownMsg 6380)

  putStrLn "   • Postgres ✓   Redis 6379 ✓   Redis 6380 ✓\n"

redisDownMsg :: Int -> String
redisDownMsg port =
  unlines
    [ "",
      "❌ Redis is not reachable on localhost:" <> show port <> ".",
      "   Start both cloud instances, then re-run `cabal run tests`:",
      "       redis-server --port 6379 --daemonize yes",
      "       redis-server --port 6380 --daemonize yes"
    ]

redisCfg :: ET.KVDBConfig
redisCfg = ET.mkKVDBConfig "KVRedis" ET.defaultKVDBConnConfig

redisCfgSecondary :: ET.KVDBConfig
redisCfgSecondary = ET.mkKVDBConfig "KVRedisSecondary" (ET.defaultKVDBConnConfig {ET.connectPort = 6380})

redisUp :: ET.KVDBConfig -> Text -> IO Bool
redisUp cfg nm = do
  e <-
    try $
      R.withFlowRuntime Nothing $ \rt ->
        I.runFlow rt $ do
          _ <- L.initKVDBConnection cfg
          r <- L.runKVDB nm (L.rawRequest ["PING"])
          pure $ either (const False) (const True) (r :: Either ET.KVDBReply ByteString)
  pure $ either (\(_ :: SomeException) -> False) id e
