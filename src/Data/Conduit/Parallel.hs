{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

{-|
Module      : Data.Conduit.Parallel
Description : Parallel conduits, using Async, MVars, and UnliftIO
Copyright   : (c) Brian Hurt, 2020
License     : BSD 3-clause
Maintainer  : bhurt42@gmail.com
Stability   : experimental

This library implements a parallel version of the very useful Data.Conduits
library.  Each stage of conduit is executed in it's own thread, concurrent
with the other stages.  This allows the whole process to overlap I/Os and
use multiple cores to perform computation.  In addition, the parallel
conduit can tee, and perform multiple paths in parallel.  This allows us
to capture a large number of patterns of parallel computation in a single
library.

We provide a way to lift normal Conduits into parallel conduits, allows us
to access the rich ecosystem of the Conduit library.  The Async library
is used to spawn the threads, so exceptions are handled correctly
(exceptions that occur in any of the sub-threads are propogated to the
main thread, and all the other sub-threads are cancelled).  We use
UnliftIO so that many different monads can be supported.

-}
module Data.Conduit.Parallel(

    -- * The ParConduit Type
    --
    -- | And running a parallel conduit.
    ParConduit,
    runParConduit,

    -- * Converting normal Conduits to ParConduits
    --
    -- | By lifting Conduits into ParConduits we take advantage of the
    -- rich Conduit ecosystem.
    liftConduit,
    liftConduitAll,

    -- * Combining ParConduits
    --
    -- | parFuse and variants, similar to Conduit's fuse function.
    parFuse,
    parFuseUpstream,
    parFuseBoth,
    parFuseS,

    -- * The ParT monad transformer
    --
    -- | The normal way to create a ParConduit is to convert a normal
    -- Conduit into a ParConduit.  But some times we know we're only
    -- going to be in ParConduit, and we don't want the "overhead"
    -- (note the scare quotes) of Conduit, so we provide an alternative
    -- interface.
    Complete(..),
    HasConduit(..),
    ParT,
    liftParT
) where

    import qualified Control.Applicative         as M
    import qualified Control.Monad               as M
    import qualified Control.Monad.Base          as M
    import qualified Control.Monad.Fail          as M
    import qualified Control.Monad.Fix           as M
    import           Control.Monad.Trans
    import           Control.Monad.Trans.Cont
    import qualified Control.Monad.Trans.Control as M
    import           Control.Monad.Trans.Reader
    import qualified Control.Monad.Zip           as M
    import           Data.Conduit                (Void)
    import qualified Data.Conduit                as C
    import           GHC.Generics
    import           UnliftIO

    -- | Prevent boolean blindness on whether the downstream ParConduit
    -- is still consuming input.
    data Complete =
        Complete    -- ^ Downstream is not still consuming input- further
                    -- input sent downstream will be discarded.
        | Ongoing -- ^ Downstream is still consuming input.
        deriving (Show, Read, Ord, Eq, Enum, Bounded, Typeable, Generic)

    -- | Internal structure to handle the arguments we pass around.
    --
    -- Rather than pass some data structure (like a raw MVar) around,
    -- we wrap the data structure up into closures, allowing us to
    -- have different structures for different situations.  This
    -- saves us from needing one structure for all situations.
    data Ops i o = Ops {
        -- | Get a value from upstream.  Nothing means the upstream
        -- has completed and no more values will be provided.
        opsConsume :: IO (Maybe i),

        -- | Send a value to downstream.  If this function returns
        -- Complete, that means the downstream has completed and
        -- all further produced values will be discarded.
        opsProduce :: o -> IO Complete,

        -- | Call when the thread will no longer be consuming more
        -- input.  This normally signals the upstream ParConduit and
        -- consumes and discards all remaining input.
        opsConsumeComplete :: IO (),

        -- | Call when the thread will no longer be produce more
        -- output.  This normally sends a Nothing to the downstream
        -- opsConsume.
        opsProduceComplete :: IO ()
    }

    -- | A ParConduit segment.
    --
    -- The type is modeled off the Conduit type.
    --
    -- We do not supply Applicative or Monad instances for this
    -- type- the implementation is difficult and the semantics would
    -- be complicated and surprising.  If you want to do this, do
    -- it with a normal Conduit, and then convert the Conduit into
    -- a ParConduit.
    newtype ParConduit i o m r = 
        ParConduit {
            -- Good resource on the ContT monad:
            -- https://ro-che.info/articles/2019-06-07-why-use-contt
            -- Using contT lets us combine withAsync calls.
            -- Also note the return type of m (Async r)- that it's
            -- a monadic action is important.  It lets us await
            -- asyncs we're not returning.  This says that there
            -- are two phases: the phase where we spawn all the
            -- threads we need, and the phase where we await all
            -- but one of the threads we spawned.
            getParConduit :: forall s .  Ops i o -> ContT s m (m (Async r))
            }
        deriving (Typeable)

    -- We can't auto-derive the Functor implemenation.  Which doesn't
    -- surprise me, it took me several tries to get it right.
    instance Functor m => Functor (ParConduit i o m) where
        fmap f pc = ParConduit $ (fmap . fmap) (fmap f) . getParConduit pc

    -- | Spawn a thread using withSync.
    --
    -- Factoring this out into it's own function just makes my head
    -- hurt less.
    spawn :: forall m a r . MonadUnliftIO m
                => m a
                -> ContT r m (Async a)
    spawn act = ContT $ withAsync act

    -- | A simple ParConduit.
    --
    -- Factored out to eliminate duplication.  Used in the simple
    -- (leaf) case where all we do is spawn a single thread.
    justSpawn :: forall i o m r . MonadUnliftIO m
                => (Ops i o -> m r)
                -> ParConduit i o m r
    justSpawn act = ParConduit $ \ops -> return <$> spawn (act ops)


    class Monad m => HasConduit i o m | m -> i, m -> o where
        consume :: m (Maybe i)
        default consume :: (HasConduit i o n, MonadTrans t, t n ~ m)
                        => m (Maybe i)
        consume = lift consume
        produce :: o -> m Complete
        default produce :: (HasConduit i o n, MonadTrans t, t n ~ m)
                        => o -> m Complete
        produce = lift . produce
        {-# MINIMAL consume, produce #-}

    newtype ParT i o m a = ParT { getParT :: ReaderT (Ops i o) m a }
        deriving (Functor, Applicative, Monad, M.MonadFix, M.MonadFail,
                    M.MonadZip, MonadIO, M.Alternative, M.MonadPlus)

    -- Can not auto-derive this?  Wha??
    instance MonadTrans (ParT i o) where
        lift = ParT . lift

    instance MonadUnliftIO m => MonadUnliftIO (ParT i o m) where
        askUnliftIO = ParT $ f <$> askUnliftIO
            where
                f :: UnliftIO (ReaderT (Ops i o) m) -> UnliftIO (ParT i o m)
                f muio = UnliftIO $ unliftIO muio . getParT

    instance M.MonadTransControl (ParT i o) where
        type StT (ParT i o) a = M.StT (ReaderT (Ops i o)) a
        liftWith = M.defaultLiftWith ParT getParT
        restoreT = M.defaultRestoreT ParT

    instance M.MonadBase b m => M.MonadBase b (ParT i o m) where
        liftBase = ParT . M.liftBase

    instance M.MonadBaseControl b m => M.MonadBaseControl b (ParT i o m) where
        type StM (ParT i o m) a = M.ComposeSt (ParT i o) m a
        liftBaseWith = M.defaultLiftBaseWith
        restoreM = M.defaultRestoreM

    instance MonadIO m => HasConduit i o (ParT i o m) where
        consume = do 
            ops <- ParT ask
            liftIO $ opsConsume ops
        produce o = do
            ops <- ParT ask
            liftIO $ opsProduce ops o

    liftParT :: forall i o m r . MonadUnliftIO m
                    => ParT i o m r -> ParConduit i o m r
    liftParT part = justSpawn go
        where
            go :: Ops i o -> m r
            go ops = do
                r <- runReaderT (getParT part) ops
                liftIO $ do
                    opsProduceComplete ops
                    opsConsumeComplete ops
                return r


    runParConduit :: forall m r . MonadUnliftIO m
                        => ParConduit () Void m r -> m r
    runParConduit pc = runContT (f ops) go
        where
            f :: Ops () Void -> ContT r m (m (Async r))
            f = getParConduit pc

            ops :: Ops () Void
            ops = Ops {
                    opsConsume = return Nothing,
                    opsProduce = \_ -> return Complete,
                    opsConsumeComplete = return (),
                    opsProduceComplete = return () }

            go :: m (Async r) -> m r
            go act = act >>= wait

    liftConduitInternal :: forall i o m r . MonadUnliftIO m
                => (Complete -> Complete)
                -> C.ConduitT i o m r
                -> ParConduit i o m r
    liftConduitInternal f cond = justSpawn go
        where
            go :: Ops i o -> m r
            go ops = do
                r <- C.connect (csource (opsConsume ops))
                            (C.fuseUpstream cond
                                (csink (opsProduce ops)))
                liftIO $ do
                    opsProduceComplete ops
                    opsConsumeComplete ops
                return r

            csource :: IO (Maybe i) -> C.ConduitT () i m ()
            csource inp = do
                r <- liftIO inp
                case r of
                    Nothing -> return ()
                    Just x -> do
                        C.yield x
                        csource inp

            csink :: MonadUnliftIO m
                    => (o -> IO Complete)
                    -> C.ConduitT o Void m ()
            csink outp = do
                r <- C.await
                case r of
                    Nothing -> return ()
                    Just x -> do
                        b <- liftIO $ outp x
                        case f b of
                            Ongoing  -> csink outp
                            Complete -> return ()

    liftConduit :: forall i o m r . MonadUnliftIO m
                => C.ConduitT i o m r
                -> ParConduit i o m r
    liftConduit = liftConduitInternal id

    liftConduitAll :: forall i o m r . MonadUnliftIO m
                => C.ConduitT i o m r
                -> ParConduit i o m r
    liftConduitAll = liftConduitInternal (const Ongoing)

    parFuseInternal :: forall i o x m r1 r2 r3 .
                    MonadUnliftIO m
                    => (m (Async r1) -> m (Async r2) -> m (Async r3))
                    -> ParConduit i x m r1
                    -> ParConduit x o m r2
                    -> ParConduit i o m r3
    parFuseInternal fixup c1 c2 = ParConduit go
        where
            go :: forall s .  Ops i o -> ContT s m (m (Async r3))
            go ops = do
                signalRef :: IORef Complete <- lift $ newIORef Ongoing
                container :: MVar (Maybe x) <- lift newEmptyMVar
                let leftOps :: Ops i x
                    leftOps = Ops {
                                opsConsume = opsConsume ops,
                                opsProduce = fuseProduce signalRef container,
                                opsConsumeComplete = opsConsumeComplete ops,
                                opsProduceComplete = fuseProduceComplete
                                                        container }

                    rightOps :: Ops x o
                    rightOps = Ops {
                                opsConsume = fuseConsume container,
                                opsProduce = opsProduce ops,
                                opsConsumeComplete = fuseConsumeComplete
                                                        signalRef container,
                                opsProduceComplete = opsProduceComplete ops }
                m1 :: m (Async r1) <- getParConduit c1 leftOps
                m2 :: m (Async r2) <- getParConduit c2 rightOps
                return $ fixup m1 m2

            fuseProduce :: IORef Complete
                            -> MVar (Maybe x)
                            -> x
                            -> IO Complete
            fuseProduce iref mvar o =
                    whenOngoing $ do
                        putMVar mvar (Just o)
                        whenOngoing $
                            return Ongoing
                where
                    whenOngoing act = do
                        r <- readIORef iref
                        case r of
                            Complete -> return Complete
                            Ongoing -> act

            fuseProduceComplete :: MVar (Maybe x)
                                -> IO ()
            fuseProduceComplete mvar =
                putMVar mvar Nothing

            fuseConsume :: MVar (Maybe x)
                        -> IO (Maybe x)
            fuseConsume mvar = do
                r <- takeMVar mvar
                case r of
                    Nothing -> do
                        putMVar mvar Nothing
                        return Nothing
                    Just _ -> return r

            fuseConsumeComplete :: IORef Complete
                                -> MVar (Maybe x)
                                -> IO ()
            fuseConsumeComplete ioref mvar = do
                    writeIORef ioref Complete
                    consumeAll
                where
                    consumeAll = do
                        r <- takeMVar mvar
                        case r of
                            Nothing -> return ()
                            Just _ -> consumeAll


    waitFirst :: forall r1 r2 r m . MonadUnliftIO m
                => (r1 -> r2 -> r)
                -> m (Async r1)
                -> m (Async r2)
                -> m (Async r)
    waitFirst f ma1 ma2 = do
        a1 <- ma1
        a2 <- ma2
        r1 <- wait a1
        return $ f r1 <$> a2

    parFuse :: forall i o x m r .
                    MonadUnliftIO m
                    => ParConduit i x m ()
                    -> ParConduit x o m r
                    -> ParConduit i o m r
    parFuse = parFuseInternal (waitFirst (flip const))

    parFuseUpstream :: forall i o x m r .
                    MonadUnliftIO m
                    => ParConduit i x m r
                    -> ParConduit x o m ()
                    -> ParConduit i o m r
    parFuseUpstream = parFuseInternal go
        where
            go :: m (Async r)
                -> m (Async ())
                -> m (Async r)
            go m1 m2 = do
                a1 <- m1
                a2 <- m2
                () <- wait a2
                return a1

    parFuseBoth :: forall i o x m r1 r2 .
                    MonadUnliftIO m
                    => ParConduit i x m r1
                    -> ParConduit x o m r2
                    -> ParConduit i o m (r1, r2)
    parFuseBoth = parFuseInternal (waitFirst (,))

    parFuseS :: forall i o x m r .
                    (MonadUnliftIO m, Semigroup r)
                    => ParConduit i x m r
                    -> ParConduit x o m r
                    -> ParConduit i o m r
    parFuseS = parFuseInternal (waitFirst (<>))


{-
    tee :: forall i m .  MonadUnliftIO m
            => ParConduit i Void m ()
            -> ParConduit i i m ()

    merge :: forall i m . MonadUnliftIO m
            => ParConduit () i m ()
            -> ParConduit i i m ()

    fanout :: forall i o m . MonadUnliftIO m
            => Int
            -> ParConduit i o m ()
            -> ParConduit i o m ()

    overlap :: forall a b m . MonadUnliftIO m
            => Int
            -> (a -> m b)
            -> ParConduit a b m ()

-}
