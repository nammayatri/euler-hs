{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- =============================================================================
-- EncodingTests — strict unit coverage for the value codec the KV connector
-- stores in Redis:
--   * EulerHS.KVConnector.Encoding.encode_  (self-describing 4-byte JSON/CBOR header)
--   * encodeDead / decodeLiveOrDead         (the live/tombstone marker)
--   * EulerHS.KVConnector.Compression       (zstd + base64 round-trips)
--
-- These are exercised with EXACT round-trip assertions (decode (encode x) == x)
-- and header-byte checks, so a regression in the codec (wrong header, broken
-- dead-marker, lossy compression) fails here even though the live KV suites run
-- with cerealEnabled=False / COMPRESSION_ALLOWED off.
-- =============================================================================
module EncodingTests (runEncodingTests) where

import qualified Data.Aeson as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Serialize as Cereal
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified EulerHS.KVConnector.Compression as Z
import EulerHS.KVConnector.Encoding (decodeLiveOrDead, encodeDead, encode_)
import Prelude

chk :: String -> Bool -> IO Bool
chk label ok = do
  putStrLn $ "  " <> (if ok then "[PASS] " else "[FAIL] ") <> label
  pure ok

runEncodingTests :: IO Bool
runEncodingTests = do
  putStrLn "\n############################################################"
  putStrLn "#  encoding + compression (JSON/CBOR/DEAD headers, zstd)"
  putStrLn "############################################################"

  let sample = ("hello-world" :: T.Text, 42 :: Int)
      jsonEnc = encode_ False sample -- "JSON" <> aeson
      cborEnc = encode_ True ("cbor-me" :: T.Text) -- "CBOR" <> cereal
  pureOks <-
    sequence
      [ chk "encode_ False ⇒ 'JSON' header" $ BSL.take 4 jsonEnc == "JSON",
        chk "encode_ False ⇒ aeson round-trips the value" $ A.decode (BSL.drop 4 jsonEnc) == Just sample,
        chk "encode_ True ⇒ 'CBOR' header" $ BSL.take 4 cborEnc == "CBOR",
        chk "encode_ True ⇒ cereal round-trips the value" $
          Cereal.decode (BSL.toStrict (BSL.drop 4 cborEnc)) == Right ("cbor-me" :: T.Text),
        chk "decodeLiveOrDead: a live value reads as live, bytes intact" $
          decodeLiveOrDead jsonEnc == (True, jsonEnc),
        chk "encodeDead ⇒ decodeLiveOrDead reads DEAD + recovers the live body" $
          decodeLiveOrDead (encodeDead jsonEnc) == (False, jsonEnc),
        chk "DEAD marker is exactly the 4-byte prefix" $ BSL.take 4 (encodeDead jsonEnc) == "DEAD",
        chk "a live JSON value is NOT misread as dead" $ fst (decodeLiveOrDead jsonEnc)
      ]

  -- zstd byte-level round-trip on a compressible payload
  let payload = TE.encodeUtf8 (T.replicate 100 "compress-me-zstd-") -- ~1.7 KB, highly repetitive
  cz <- Z.compress (Just 3) payload
  zOk <- case cz of
    Left _ -> chk "compress: zstd compresses large input" False
    Right comp -> do
      a <- chk "compress: output is smaller than the input" (BS.length comp < BS.length payload)
      dz <- Z.decompress comp
      b <- chk "decompress: round-trips back to the original bytes" (either (const False) (== payload) dz)
      pure (a && b)

  -- NOTE: Compression.compressObjectIO / decompressObjectIO (the base64 helpers)
  -- are deliberately NOT tested here — they are unused by the connector AND have a
  -- real asymmetry bug (compressObjectIO stores sub-512-byte objects as raw JSON,
  -- but decompressObjectIO always base64-decodes, so small objects do not round
  -- trip). The connector's live path uses `compress`/`decompress` (above) via
  -- compressWithoutError for the drain stream; the decompress side lives in the
  -- external drainer. See the audit notes for the bug report.

  let oks = pureOks <> [zOk]
      passed = length (filter id oks)
  putStrLn $ "\n=== encoding+compression: " <> show passed <> "/" <> show (length oks) <> " passed ==="
  pure (and oks)
