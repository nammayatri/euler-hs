{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module EulerHS.KVConnector.Compression where

import qualified Codec.Compression.Zstd as Zstd
import qualified Data.Aeson as A
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified EulerHS.Language as L
import EulerHS.Prelude
import EulerHS.Types

-- Data structure to hold compression parameters
data CompressionParams = CompressionParams
  { compressionLevel :: Int,
    threshold :: Int
  }
  deriving stock (Generic, Typeable, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)

-- Key used to fetch CompressionParams
data CompressionParamsKey = CompressionParamsKey
  deriving stock (Generic, Typeable, Show, Eq)
  deriving anyclass (ToJSON, FromJSON)

instance OptionEntity CompressionParamsKey CompressionParams

-- Error type for compression failures
data CompressionError = CompressionError String BS.ByteString
  deriving (Show)

-- Error type for decompression failures
data DecompressionError = DecompressionError String BS.ByteString
  deriving (Show)

-- | Retrieve compression parameters, using a default if not found
getCompressionParams :: (L.MonadFlow m) => m CompressionParams
getCompressionParams = fromMaybe (CompressionParams 2 512) <$> L.getOption CompressionParamsKey

-- | Compress the input ByteString
compress :: Maybe Int -> BS.ByteString -> IO (Either CompressionError BS.ByteString)
compress compressionLevel' input = do
  let compressionLevel'' = fromMaybe 2 compressionLevel'
  result <- try (pure $ Zstd.compress compressionLevel'' input) :: IO (Either SomeException BS.ByteString)
  case result of
    Left err -> pure $ Left (CompressionError ("Compression error: " <> show err) input)
    Right output -> pure $ Right output

-- | Decompress the input ByteString
decompress :: BS.ByteString -> IO (Either DecompressionError BS.ByteString)
decompress input = do
  result <- try (pure $ Zstd.decompress input) :: IO (Either SomeException Zstd.Decompress)
  case result of
    Left _ -> pure $ Right input
    Right Zstd.Skip -> pure $ Right input
    Right (Zstd.Error err) -> pure $ Left (DecompressionError ("Decompression error: " <> err) input)
    Right (Zstd.Decompress output) -> pure $ Right output

-------------------------------------------------
-- Compression and Decompression of Objects
-------------------------------------------------

compressObject :: (ToJSON a, L.MonadFlow m) => a -> m T.Text
compressObject obj = do
  compressionParams <- getCompressionParams
  let strictByteString = BL.toStrict (A.encode obj)
  if BS.length strictByteString < threshold compressionParams
    then return (TE.decodeUtf8 strictByteString)
    else do
      compressResult <- L.runIO $ compress (Just $ compressionLevel compressionParams) strictByteString
      case compressResult of
        Left (CompressionError err _) -> do
          L.logError @T.Text "Error while compressing object: " $ T.pack err
          return $ TE.decodeUtf8 strictByteString
        Right compressed -> return $ TE.decodeUtf8 $ B64.encode compressed

decompressObject :: (FromJSON a, L.MonadFlow m) => T.Text -> m (Maybe a)
decompressObject compressedObj = do
  case B64.decode (TE.encodeUtf8 compressedObj) of
    Left err -> do
      L.logError @T.Text "Error while decoding Base64: " $ T.pack err
      return Nothing
    Right strictByteString -> do
      decompressResult <- L.runIO $ decompress strictByteString
      case decompressResult of
        Left (DecompressionError err _) -> do
          L.logError @T.Text "Error while decompressing object: " $ T.pack err
          return $ A.decode (BL.fromStrict strictByteString)
        Right decompressed -> return $ A.decode (BL.fromStrict decompressed)

-------------------------------------------------
-- Compression and Decompression with IO monad
-------------------------------------------------

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
      friends = replicate 3 (Person "Friend" 30 (Address "123 Friend St" "Friend City" "12345") [])
    }

compressionThreshold :: Int
compressionThreshold = 512

-- | Test compress and decompress function
testCompressDecompress :: IO ()
testCompressDecompress = do
  when (length (A.encode person) > compressionThreshold) $ do
    compressed <- compressObjectIO person -- Compress the object
    print $ "Length of compressed object: " ++ show (T.length compressed)
    -- print $ "Compressed Object: " ++ T.unpack compressed
    decompressed <- decompressObjectIO compressed -- Decompress the object
    case decompressed of
      Just person' -> do
        -- print $ "Decompressed Object: " ++ show (person' :: Person)
        print $ "Length of decompressed object: " ++ show (length $ BL.toStrict $ A.encode (person' :: Person))
      Nothing -> print ("Failed to decompress" :: T.Text)
  return ()
