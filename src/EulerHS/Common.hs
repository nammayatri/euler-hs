{-# LANGUAGE DerivingVia #-}

module EulerHS.Common
  (
    -- * Guid for any flow
    FlowGUID
    -- * Guid for a forked flow
  , ForkGUID
    -- * Guid for a safe flow
  , SafeFlowGUID
    -- * Network manager selector
  , ManagerSelector(..)
    -- * Description type
  , Description
    -- * A variable for await results from a forked flow
  , Awaitable (..)
  , Microseconds (..)
  , cancelAwaitable
  ) where

import qualified Control.Concurrent as C
import qualified Data.Word as W
import           EulerHS.Prelude

type FlowGUID = Text
type ForkGUID = Text
type SafeFlowGUID = Text

newtype ManagerSelector = ManagerSelector Text
  deriving (Eq, IsString) via Text
  deriving stock (Show)

type Description = Text

-- | Result handle for a forked flow.  Carries the 'ThreadId' so callers can
-- cancel the forked work via 'cancelAwaitable' if they no longer need the
-- result (e.g. the caller already has a complete answer from another source).
data Awaitable s = Awaitable C.ThreadId (MVar s)

data Microseconds = Microseconds W.Word32 -- Max timeout ~71 minutes with Word32

-- | Cancel the forked thread backing an 'Awaitable' by delivering an async
-- 'C.ThreadKilled' exception.  Safe to call multiple times.  Any 'await' on
-- this 'Awaitable' after cancellation will observe the killed thread's
-- 'SafeFlow' result as a 'Left' error.
cancelAwaitable :: Awaitable s -> IO ()
cancelAwaitable (Awaitable tid _) = C.killThread tid
