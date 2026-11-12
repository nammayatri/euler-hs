{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- | Optional per-request IO profiling hook.
--
-- The embedding application puts an 'IOProfileSink' into the request's local
-- options under 'IOProfileSinkKey'. The interpreter then reports every
-- physical SQL / KV-Redis call to it. euler knows nothing about where the
-- events go; with no sink installed, nothing is reported.
module EulerHS.Extra.IOProfile
  ( IOEvent (..),
    IOKind (..),
    IOProfileSink (..),
    IOProfileSinkKey (..),
  )
where

import EulerHS.Prelude
import EulerHS.Types (OptionEntity)

data IOKind = SqlCall | KvRedisCall
  deriving stock (Eq, Show, Generic)

data IOEvent = IOEvent
  { ioKind :: IOKind,
    -- | SQL statements for 'SqlCall' (joined by "; "); connection name for 'KvRedisCall'.
    ioTarget :: Text,
    -- | Wall-clock start and end, in milliseconds.
    ioStartMs :: Double,
    ioEndMs :: Double,
    -- | Haskell thread that ran the call (shown form of ThreadId).
    ioThread :: Text,
    ioOk :: Bool
  }
  deriving stock (Show)

newtype IOProfileSink = IOProfileSink {reportIOEvent :: IOEvent -> IO ()}

data IOProfileSinkKey = IOProfileSinkKey
  deriving stock (Eq, Show, Generic, Ord)
  deriving anyclass (FromJSON, ToJSON)

instance OptionEntity IOProfileSinkKey IOProfileSink
