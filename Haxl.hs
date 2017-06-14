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

  (>>) = (*>)



instance Applicative Haxl where
  pure = return

  Haxl fio <*> Haxl aio = Haxl $ \sched -> do
    rf <- fio sched
    ra <- aio sched

    case (rf, ra) of
      (Done f, Done a) ->
        return (Done (f a))

      (Done f, Blocked sync a_cont) ->
        return (Blocked sync (\b -> f <$> a_cont b))

      (Blocked f_sync f_cont, Done a) ->
        return (Blocked f_sync (\b -> f_cont b <*> return a))

      (Blocked f_sync f_cont, Blocked a_sync a_cont) -> do
        sync <- newSync
        blockOn f_sync (WaitingFor f_cont sync)
        let
          cont b = do
            a <- a_cont b
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
  Sync result <- newSync         -- where to put the result
  let
    schedule :: SchedState -> [Ready] -> IO a
    schedule sched (Ready (Haxl io) sync@(Sync ref) : ready) = do
      r <- io sched
      case r of
        Done a -> putSync sched sync a
        Blocked sync1 cont -> do
          blockOn sync1 (WaitingFor cont sync)
          schedule sched ready
    schedule sched [] = do
      Complete sync val <- atomically $ do
        comps <- readTVar (completions sched)
        case comps of
          [] -> retry
          (c:cs) -> do
            writeTVar (completions sched) cs
            return c
      putSync sched sync val

    putSync :: SchedState -> Sync b -> b -> IO a
    putSync sched (Sync ref) val = do
      contents <- readIORef ref
      case contents of
        SyncFull _ -> error "double put"
        SyncEmpty waiting -> do
          writeIORef ref (SyncFull val)
          case sameIORef ref result of
            Just Same -> return val
            Nothing -> schedule sched (map (`unblock` val) waiting)

  completions <- newTVarIO []
  schedule (SchedState completions) [Ready haxl (Sync result)]



data Same a b where
  Same :: Same a a

sameIORef :: IORef a -> IORef b -> Maybe (Same a b)
sameIORef ref1 ref2
  | ref1 == unsafeCoerce ref2 = Just (unsafeCoerce Same)
  | otherwise = Nothing

-- -----------------------------------------------------------------------------
-- Perform some I/O

overlapIO :: IO a -> Haxl a
overlapIO io =
  Haxl $ \SchedState{..} -> do
    Sync ref <- newSync
    forkIO $ do
      a <- io
      atomically $ do
        cs <- readTVar completions
        writeTVar completions (Complete (Sync ref) a : cs)
    return (Blocked (Sync ref) return)


-- -----------------------------------------------------------------------------
-- Examples

-- | wait one second and then print a character
test :: Char -> Haxl Char
test c = overlapIO $ do
  printf "%c:start\n" c
  threadDelay 1000000
  printf "%c:end\n" c
  return c

ex1 = runHaxl $ do sequence [ test n | n <- "abc" ]

-- >> is sequential
ex2 = runHaxl $ annotate 2 $
  sequence [ test 'a' >>= \_ -> test 'b'
           , test 'c' >>= \_ -> test 'd' ]

-- Test ApplicativeDo
ex3 = runHaxl $ annotate 3 $ do
  a <- test 'a'
  b <- test (const 'b' a)
  c <- test 'c'
  d <- test (const 'd' c)
  e <- test (const 'e' (b,d))
  return d


annotate :: Int -> Haxl a -> Haxl a
annotate n haxl = separators *> (overlapIO (threadDelay 500000) >>= \_ -> haxl)
  where
    separators = overlapIO $ do
      forM_ [1..n] $ \m -> do
        threadDelay 1000000
        printf "\n%d -----\n" m


softwareUpdate = runHaxl $ annotate 3 $ do
  latest <- getLatestVersion
  hosts <- getHosts
  installed <- mapM getInstalledVersion hosts
  let updates = [ h | (h,v) <- zip hosts installed, v < latest ]
  mapM_ (updateTo latest) updates



getLatestVersion :: Haxl Int
getLatestVersion = overlapIO $ do
  putStrLn "getLatestVersion:start"
  threadDelay 2000000
  putStrLn "getLatestVersion:done"
  return 3

getHosts :: Haxl [String]
getHosts = overlapIO $ do
  putStrLn "getHosts:start"
  threadDelay 1000000
  putStrLn "getHosts:done"
  return ["host1", "host2", "host3"]

getInstalledVersion :: String -> Haxl Int
getInstalledVersion h = overlapIO $ do
  putStrLn "getInstalledVersion:start"
  threadDelay 1000000
  putStrLn "getInstalledVersion:done"
  return (read (drop 4 h))

updateTo :: Int -> String -> Haxl ()
updateTo v h = overlapIO $ do
  putStrLn "updateTo:start"
  threadDelay 1000000
  putStrLn "updateTo:done"
  return ()
