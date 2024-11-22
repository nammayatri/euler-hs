{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module EulerHS.KVConnector.Compression where

import qualified Codec.Compression.Zstd as Zstd
import qualified Data.Aeson as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Char8 as CBS
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified EulerHS.Language as L
import EulerHS.Prelude
import EulerHS.Types
import qualified Juspay.Extra.Config as Conf

isCompressionAllowed :: Bool
isCompressionAllowed = fromMaybe False $ readMaybe =<< Conf.lookupEnvT @String "COMPRESSION_ALLOWED"

isCompressionMetricsEnabled :: Bool
isCompressionMetricsEnabled = fromMaybe False $ readMaybe =<< Conf.lookupEnvT @String "COMPRESSION_METRICS_ENABLED"

-- Data structure to hold compression parameters
data CompressionParams = CompressionParams
  { compressionLevel :: Int,
    sizeThreshold :: Int
  }
  deriving stock (Generic, Typeable, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)

defaultCompressionParams :: CompressionParams
defaultCompressionParams = CompressionParams 2 256

-- Hashmap to hold compression tables
data CompressionTables = CompressionTables (HM.HashMap T.Text CompressionParams)
  deriving stock (Generic, Typeable, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)

-- Key used to fetch CompressionParams
data CompressionTableParams = CompressionTableParams
  deriving stock (Generic, Typeable, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)

instance OptionEntity CompressionTableParams CompressionTables

-- Error type for compression failures
data CompressionError = CompressionError String BS.ByteString
  deriving (Show)

-- Error type for decompression failures
data DecompressionError = DecompressionError String BS.ByteString
  deriving (Show)

-- | Retrieve compression parameters, using a default if not found
getCompressionParamsForTable :: (L.MonadFlow m) => Maybe T.Text -> m CompressionParams
getCompressionParamsForTable Nothing = pure defaultCompressionParams
getCompressionParamsForTable (Just tble) = do
  compressionTables <- L.getOption CompressionTableParams
  case compressionTables of
    Just (CompressionTables tables) -> pure $ HM.lookupDefault defaultCompressionParams tble tables
    Nothing -> pure defaultCompressionParams

-- | Compress the input ByteString
compress :: Maybe Int -> BS.ByteString -> IO (Either CompressionError BS.ByteString)
compress compressionLevel' input = do
  let compressionLevel'' = fromMaybe 2 compressionLevel'
  result <- try (pure $ Zstd.compress compressionLevel'' input) :: IO (Either SomeException BS.ByteString)
  case result of
    Left err -> pure $ Left (CompressionError ("Compression error: " <> show err) input)
    Right output -> pure $ Right output

compressWithoutError :: (L.MonadFlow m) => Maybe Text -> BS.ByteString -> m BS.ByteString
compressWithoutError table input = do
  compressionParams <- getCompressionParamsForTable table
  if BS.length input < sizeThreshold compressionParams
    then pure input
    else do
      result <- L.runIO $ compress (Just compressionParams.compressionLevel) input
      case result of
        Left err -> do
          L.logError @T.Text "Error while compressing object: " $ T.pack $ show err
          pure input
        Right output -> pure output

-- | Decompress the input ByteString
decompress :: BS.ByteString -> IO (Either DecompressionError BS.ByteString)
decompress input = do
  result <- try (pure $ Zstd.decompress input) :: IO (Either SomeException Zstd.Decompress)
  case result of
    Left _ -> pure $ Right input
    Right Zstd.Skip -> pure $ Right input
    Right (Zstd.Error err) -> pure $ Left (DecompressionError ("Decompression error: " <> err) input)
    Right (Zstd.Decompress output) -> pure $ Right output

decompressWithoutError :: BS.ByteString -> IO BS.ByteString
decompressWithoutError input = do
  result <- try (pure $ Zstd.decompress input) :: IO (Either SomeException Zstd.Decompress)
  case result of
    Left _ -> pure input
    Right Zstd.Skip -> pure input
    Right (Zstd.Error _) -> pure input
    Right (Zstd.Decompress output) -> pure output

-------------------------------------------------
-- Compression and Decompression with IO monad
-------------------------------------------------

compressIO :: Maybe Int -> String -> IO BS.ByteString
compressIO compressionLevel' input = do
  let inputBS = CBS.pack input
  result <- compress compressionLevel' inputBS
  case result of
    Left (CompressionError err _) -> do
      putStrLn $ "Error while compressing object: " ++ err
      return inputBS
    Right compressed -> return compressed

decompressIO :: BS.ByteString -> IO String
decompressIO compressedObj = do
  decompressResult <- decompressWithoutError compressedObj
  return $ CBS.unpack decompressResult

-------------------------------------------------
-- Test Cases
-------------------------------------------------

data Address = Address
  { street :: String,
    city :: String,
    postalCode :: String
  }
  deriving (Show, Generic)

data Person = Person
  { name :: String,
    age :: Int,
    address :: Address,
    friends :: [Person] -- Recursive structure for a large list of friends
  }
  deriving (Show, Generic)

instance ToJSON Address

instance FromJSON Address

instance ToJSON Person

instance FromJSON Person

person :: Person
person =
  Person
    { name = "John Doe",
      age = 45,
      address =
        Address
          { street = "123 Main St",
            city = "Metropolis",
            postalCode = "12345"
          },
      friends = replicate 7 (Person "Friend" 30 (Address "123 Friend St" "Friend City" "12345") [])
    }

compressionThreshold :: Int
compressionThreshold = 512

compressObjectIO :: (ToJSON a) => a -> IO T.Text
compressObjectIO obj = do
  let encodedObj = A.encode obj
  if (length encodedObj) < compressionThreshold
    then return (TE.decodeUtf8 $ BL.toStrict encodedObj)
    else do
      let strictByteString = BL.toStrict encodedObj
      compressResult <- compress (Just 1) strictByteString
      case compressResult of
        Left (CompressionError err _) -> do
          print $ "Error while compressing object: " ++ err
          return (TE.decodeUtf8 strictByteString)
        Right compressed -> return (TE.decodeUtf8 $ B64.encode compressed)

-- | Decompress and decode from Base64
decompressObjectIO :: (FromJSON a) => T.Text -> IO (Maybe a)
decompressObjectIO compressedObj = do
  let decodedResult = B64.decode (TE.encodeUtf8 compressedObj) -- Decode Base64
  case decodedResult of
    Left err -> do
      print $ "Error while decoding Base64: " ++ err
      return Nothing -- Return Nothing if decoding fails
    Right strictByteString -> do
      decompressResult <- decompress strictByteString -- Decompress the object
      case decompressResult of
        Left (DecompressionError err _) -> do
          print $ "Error while decompressing object: " ++ err
          return $ A.decode (BL.fromStrict strictByteString) -- Return original if decompression fails
        Right decompressed -> return $ A.decode (BL.fromStrict decompressed)

-- | Test compress and decompress function
testCompressDecompress :: IO ()
testCompressDecompress = do
  when (length (A.encode person) > compressionThreshold) $ do
    compressed <- compressObjectIO person -- Compress the object
    print $ "Length of compressed object: " ++ show (T.length compressed)
    print $ "Compressed Object: " ++ T.unpack compressed
    decompressed <- decompressObjectIO compressed -- Decompress the object
    case decompressed of
      Just person' -> do
        -- print $ "Decompressed Object: " ++ show (person' :: Person)
        print $ "Length of decompressed object: " ++ show (length $ BL.toStrict $ A.encode (person' :: Person))
      Nothing -> print ("Failed to decompress" :: T.Text)
  return ()
