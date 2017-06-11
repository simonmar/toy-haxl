--
-- | A toy implementation of Haxl to illustrate the internals.  It supports
-- overlapping I/O only, there is no support for:
--
-- * batching
-- * caching
-- * memoization
-- * exceptions
-- * user data
--

{-# OPTIONS_GHC -foptimal-applicative-do #-}
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE GADTs #-}

module Haxl where

import Data.IORef
import Control.Concurrent.STM
import Control.Concurrent
import Control.Monad
import Text.Printf
import Unsafe.Coerce
import System.IO


-- -----------------------------------------------------------------------------
-- Types


-- | A synchronisation point
newtype Sync a = Sync (IORef (SyncContents a))


data SyncContents a
  = SyncFull a
  | SyncEmpty [WaitingFor a]


-- | A computation waiting for an 'a', that delivers its result 'b'
-- to another 'Sync'
data WaitingFor a where
  WaitingFor :: (a -> Haxl b) -> Sync b -> WaitingFor a


-- | The scheduler state
data SchedState =
 SchedState
   { completions :: TVar [Complete]
   }


-- | A 'Sync' and its value
data Complete where
  Complete :: Sync a -> a -> Complete


data Result a where
  Done    :: a -> Result a
  Blocked :: Sync b -> (b -> Haxl a) -> Result a


newtype Haxl a = Haxl { unHaxl :: SchedState -> IO (Result a) }



-- -----------------------------------------------------------------------------
-- Synchronisation points


-- | Create a new Sync point
newSync :: IO (Sync a)
newSync = Sync <$> newIORef (SyncEmpty [])


-- | Wait for a Sync to be full
getSync :: Sync a -> Haxl a
getSync (Sync ref) = Haxl $ \_sched -> do
  e <- readIORef ref
  case e of
    SyncFull a -> return (Done a)
    SyncEmpty _ -> return (Blocked (Sync ref) return)


-- | Block a computation on a Sync
blockOn :: Sync a -> WaitingFor a -> IO ()
blockOn (Sync ref) waitingFor =
  modifyIORef' ref $ \contents ->
    case contents of
      SyncEmpty list -> SyncEmpty (waitingFor : list)
      _ -> error "blockOn"



-- -----------------------------------------------------------------------------
-- Monad / Applicative instances


instance Functor Haxl where
  fmap f m = m >>= return . f


instance Monad Haxl where
  return a = Haxl $ \_ -> return (Done a)
  Haxl m >>= k = Haxl $ \sched -> do
    r <- m sched
    case r of
      Done a -> unHaxl (k a) sched
      Blocked sync cont -> return (Blocked sync (\b -> cont b >>= k))


instance Applicative Haxl where
  pure = return

  Haxl fio <*> Haxl aio = Haxl $ \sched -> do
    rf <- fio sched
    ra <- aio sched
    case (rf, ra) of
      (Done f, Done a) ->
        return (Done (f a))
      (Done f, Blocked sync acont) ->
        return (Blocked sync (\b -> f <$> acont b))
      (Blocked f_sync fcont, Done a) ->
        return (Blocked f_sync (\b -> fcont b <*> return a))
      (Blocked f_sync fcont, Blocked a_sync acont) -> do
        sync <- newSync
        blockOn f_sync (WaitingFor fcont sync)
        let
          cont b = do
            a <- acont b
            f <- getSync sync
            return (f a)
        return (Blocked a_sync cont)
  


-- -----------------------------------------------------------------------------
-- The scheduler


data Ready = forall a . Ready (Haxl a) (Sync a)

unblock :: WaitingFor a -> a -> Ready
unblock (WaitingFor fn sync) a = Ready (fn a) sync

runHaxl :: forall a . Haxl a -> IO a
runHaxl haxl = do
  result <- newIORef (SyncEmpty [])         -- where to put the result
  let
    schedule :: SchedState -> Haxl b -> Sync b -> [Ready] -> IO a
    schedule sched (Haxl io) sync@(Sync ref) ready = do
      r <- io sched
      case r of
        Done a -> putSync sched sync a
        Blocked sync1 cont -> do
          blockOn sync1 (WaitingFor cont sync)
          reschedule sched ready

    putSync :: SchedState -> Sync b -> b -> IO a
    putSync sched (Sync ref) val = do
      contents <- readIORef ref
      case contents of
        SyncFull _ -> error "double put"
        SyncEmpty waiting -> do
          writeIORef ref (SyncFull val)
          if ref == unsafeCoerce result
             then return (unsafeCoerce val)
             else reschedule sched (map (`unblock` val) waiting)

    reschedule :: SchedState -> [Ready] -> IO a
    reschedule sched (Ready haxl sync : ready) = schedule sched haxl sync ready
    reschedule sched [] = do
      Complete sync val <- atomically $ do
        comps <- readTVar (completions sched)
        case comps of
          [] -> retry
          (c:cs) -> do
            writeTVar (completions sched) cs
            return c
      putSync sched sync val

  completions <- newTVarIO []
  schedule (SchedState completions) haxl (Sync result) []


-- -----------------------------------------------------------------------------
-- Perform some I/O

overlapIO :: IO a -> Haxl a
overlapIO io =
  Haxl $ \SchedState{..} -> do
    Sync ref <- newSync
    forkIO $ do
      a <- io;
      atomically $ do
        cs <- readTVar completions
        writeTVar completions (Complete (Sync ref) a : cs)
    return (Blocked (Sync ref) return)


-- -----------------------------------------------------------------------------
-- Examples

-- | wait one second and then print a character
test :: Char -> Haxl Char
test c = overlapIO $ do
  threadDelay 1000000
  putStr [c]
  hFlush stdout
  return c

ex1 = runHaxl $ do sequence [ test n | n <- "abc" ]

-- >> is sequential
ex2 = runHaxl $ separators 2 *>
  sequence [ test 'a' >> test 'b'
           , test 'c' >> test 'd' ]

-- Test ApplicativeDo
ex3 = runHaxl $ separators 3 *> do
  a <- test 'a'
  b <- test (const 'b' a)
  c <- test 'c'
  d <- test (const 'd' c)
  e <- test (const 'e' (b,d))
  return e


separators n = overlapIO $ do
  threadDelay 1100000;
  replicateM_ n (do putStrLn "\n----"; threadDelay 1000000)


