{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-
{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}
-}

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
module Data.Conduit.Parallel {- (

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

    -- * Parallel Combinators
    --
    -- | Various ways to combine multiple concurrent ParConduits.
    --
    tee,
    merge,

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

) -} where

    import           Control.DeepSeq
    import           Control.Monad.Trans.Cont
    import           Data.Conduit                        (Void)
    import qualified Data.Conduit                        as C
    import           Data.Conduit.Parallel.Internal.Duct
    import           UnliftIO


{-
    import           Control.Concurrent.MVar             (MVar)



    import qualified Control.Monad            as M
    import           GHC.Generics
    import qualified Control.Monad.Zip           as M
    import           Control.Monad.Trans.Reader
    import qualified Control.Monad.Trans.Control as M
    import           Control.Monad.Trans
    import qualified Control.Monad.Fix           as M
    import qualified Control.Monad.Fail          as M
    import qualified Control.Monad.Base          as M
    import qualified Control.Concurrent.MVar     as MVar
    import           Control.Concurrent.STM      (retry)
    import           Control.Concurrent.STM      (TVar)
    import qualified Control.Concurrent.STM      as STM
    import qualified Control.Applicative         as M
-}

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
            -- Also note the return type of m r- that it's
            -- a monadic action is important.  It lets us await
            -- asyncs.  This says that there are two phases: the phase
            -- where we spawn all the threads we need, and the phase
            -- where we await all of the threads we spawned.
            getParConduit ::
                forall s .  
                    ReadDuct i
                    -> WriteDuct o
                    -> ContT s m (m r)
            }
        deriving (Typeable)

    -- | The fmap function.
    --
    -- This function is a lot easier to understand and modify if
    -- I break out and explicitly type all the sub pieces (I am a
    -- bear of very little brain).  This is easier to do as a
    -- stand alone function not part of the Functor instance.
    pcFunctor :: forall i o m a b .
                    Functor m
                    => (a -> b)
                    -> ParConduit i o m a
                    -> ParConduit i o m b
    pcFunctor f pc = ParConduit go
        where
            go :: forall s . ReadDuct i -> WriteDuct o -> ContT s m (m b)
            go rc wc = fixup (getParConduit pc rc wc)

            fixup :: forall s . ContT s m (m a) -> ContT s m (m b)
            fixup cm = fmap f <$> cm


    instance Functor m => Functor (ParConduit i o m) where
        fmap = pcFunctor


    -- | Spawn a thread using withSync.
    --
    -- Factoring this out into it's own function just makes my head
    -- hurt less.
    spawn :: forall m a r . MonadUnliftIO m
                => m a
                -> ContT r m (m a)
    spawn act = do
        a <- ContT $ withAsync act
        return (wait a)


    -- | A simple ParConduit.
    --
    -- Factored out to eliminate duplication.  Used in the simple
    -- (leaf) case where all we do is spawn a single thread.
    justSpawn :: forall i o m r . MonadUnliftIO m
                => (ReadDuct i -> WriteDuct o -> m r)
                -> ParConduit i o m r
    justSpawn act = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct o
                                -> ContT s m (m r)
            go r w = spawn (act r w)

    runParConduit :: forall m r . MonadUnliftIO m
                        => ParConduit () Void m r -> m r
    runParConduit pc = runContT
                        (getParConduit pc closedReadDuct closedWriteDuct) id

    liftConduit :: forall i o m r . 
                    (MonadUnliftIO m
                    , NFData o)
                    => C.ConduitT i o m r
                    -> ParConduit i o m r
    liftConduit cond = justSpawn go
        where
            go :: ReadDuct i -> WriteDuct o -> m r
            go rd wd = do
                r <- C.connect (csource rd)
                            (C.fuseUpstream cond (csink wd))
                liftIO $ do
                    closeReadDuct rd
                    closeWriteDuct wd
                    return r

            csource :: ReadDuct i -> C.ConduitT () i m ()
            csource rd = do
                r <- liftIO $ readDuct rd
                case r of
                    Nothing -> return ()
                    Just x -> do
                        C.yield x
                        csource rd

            csink :: WriteDuct o -> C.ConduitT o Void m ()
            csink wd = do
                r <- C.await
                case r of
                    Nothing -> return ()
                    Just x -> do
                        isc <- liftIO $ ductWrite wd x
                        case isc of
                            IsClosed -> return ()
                            IsNotClosed -> csink wd

    parFuseInternal :: forall i o x m r1 r2 r .
                    MonadUnliftIO m
                    => (r1 -> r2 -> r)
                    -> ParConduit i x m r1
                    -> ParConduit x o m r2
                    -> ParConduit i o m r
    parFuseInternal fixup c1 c2 = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct o -> ContT s m (m r)
            go outerRd outerWd = do
                (innerRd, innerWd) <- liftIO $ createDuct
                m1 :: m r1 <- getParConduit c1 outerRd innerWd
                m2 :: m r2 <- getParConduit c2 innerRd outerWd
                return $ fixup <$> m1 <*> m2

    parFuse :: forall i o x m r .
                    MonadUnliftIO m
                    => ParConduit i x m ()
                    -> ParConduit x o m r
                    -> ParConduit i o m r
    parFuse = parFuseInternal (flip const)

    parFuseUpstream :: forall i o x m r .
                    MonadUnliftIO m
                    => ParConduit i x m r
                    -> ParConduit x o m ()
                    -> ParConduit i o m r
    parFuseUpstream = parFuseInternal const

    parFuseBoth :: forall i o x m r1 r2 .
                    MonadUnliftIO m
                    => ParConduit i x m r1
                    -> ParConduit x o m r2
                    -> ParConduit i o m (r1, r2)
    parFuseBoth = parFuseInternal (\x y -> (x,y))

    parFuseS :: forall i o x m r .
                    (MonadUnliftIO m, Semigroup r)
                    => ParConduit i x m r
                    -> ParConduit x o m r
                    -> ParConduit i o m r
    parFuseS = parFuseInternal (<>)

    copier :: forall m i .
                (MonadIO m,
                NFData i)
                => ReadDuct i
                -> [ WriteDuct i ]
                -> m ()
    copier _ [] = return ()
    copier rd wds = do
            r <- liftIO $ readDuct rd
            case r of
                Nothing -> return ()
                Just x -> do
                    newWds :: [ WriteDuct i ] <- doWrites x wds
                    copier rd newWds
        where
            doWrites :: i -> [ WriteDuct i ] -> m [ WriteDuct i ]
            doWrites _ [] = return []
            doWrites x (w : ws) = do
                s <- liftIO $ ductWrite w x
                ws' <- doWrites x ws
                return $ case s of
                            IsClosed -> ws'
                            IsNotClosed -> w : ws'

    closeCopier :: forall m i .
                    (MonadIO m)
                    => ReadDuct i
                    -> [ WriteDuct i ]
                    -> m ()
    closeCopier rd wds = liftIO $ do
        closeReadDuct rd
        mapM_ closeWriteDuct wds
        return ()

    copyWithClose :: forall m i .
                        (MonadIO m
                        , NFData i)
                        => ReadDuct i
                        -> [ WriteDuct i ]
                        -> m ()
    copyWithClose rd wds = copier rd wds >> closeCopier rd wds
                    
    branch :: forall m i o .
                    (MonadUnliftIO m,
                    NFData i)
                    => ParConduit i o m ()
                    -> ParConduit i o m ()
                    -> ParConduit i o m ()
    branch path1 path2 = ParConduit go
        where
            go :: forall s . ReadDuct i -> WriteDuct o -> ContT s m (m ())
            go rd wd = do
                (rc1, wc1) <- liftIO $ createDuct
                (rc2, wc2) <- liftIO $ createDuct
                m1 <- getParConduit path1 rc1 wd
                m2 <- getParConduit path2 rc2 wd
                m3 <- spawn $ copyWithClose rd [ wc1, wc2 ]
                return $ m3 >> m1 >> m2

    tee :: forall i m . 
            (MonadUnliftIO m
            , NFData i)
            => ParConduit i Void m ()
            -> ParConduit i i m ()
    tee sink = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m ())
            go rd wd = do
                (ird, iwd) <- liftIO $ createDuct
                a1 :: m () <- getParConduit sink ird closedWriteDuct
                a2 :: m () <- spawn $ copyWithClose rd [ wd, iwd ]
                return $ const <$> a1 <*> a2

    merge :: forall i m .
            (MonadUnliftIO m
            , NFData i)
            => ParConduit () i m ()
            -> ParConduit i i m ()
    merge src = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m ())
            go ord owd = do
                doneTVar :: TVar Bool <- newTVarIO False
                m1 <- spawn $ doCopy ord owd doneTVar
                (ird, iwd) <- liftIO $ createDuct
                m2 <- getParConduit src closedReadDuct iwd
                m3 <- spawn $ doCopy ird owd doneTVar
                return $ m1 >> m2 >> m3

            doCopy :: ReadDuct i -> WriteDuct i -> TVar Bool -> m ()
            doCopy rd wd tvar = do
                copier rd [ wd ]
                liftIO $ closeReadDuct rd
                b <- liftIO . atomically $ do
                        b' <- readTVar tvar
                        writeTVar tvar True
                        return b'
                case b of
                    False -> return ()
                    True -> liftIO $ do
                                        closeWriteDuct wd
                                        return ()
                
{-
    split :: forall a b o m .
            => ParConduit a o m ()
            -> ParConduit b o m ()
            -> ParConduit (Either a b) o m ()
    split leftFork rightFork = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i
                    -> ContT s m (m (Async ()))
            go rd wd = do
                (ird, iwd) <- createDuct
                a1 :: m (Async ()) <- getParConduit sink ird noWriteDuct
                let a2 :: m (Async ()) = spawn $ copier rd wd iwd
                return $ const <$> a1 <*> a2

            copier :: ReadDuct (Either a b) -> WriteDuct a
                            -> WriteDuct b -> m ()
            copier rd wd1 wd2 = runMaybeIOUnit $ finally loop closeAll
                where
                    loop :: MaybeT IO ()
                    loop = do
                        r <- readDuctMaybe rd
                        case r of
                            Left a  -> ductWrite wd1 a
                            Right b -> ductWrite wd2 b
                        loop

    replicate :: forall i o m . MonadUnliftIO m
            => Int
            -> ParConduit i o m ()
            -> ParConduit i o m ()

    synchronous :: forall a b m . MonadUnliftIO m
                => Int
                -> (a -> m b)
                -> ParConduit a b m ()


    fan :: forall i1 i2 o m . MonadUnliftIO m
            => ParConduit i1 o m ()
            -> ParConduit i2 o m ()
            -> ParConduit (Either i1 i2) o m ()

    partition :: forall i1 i2 o m . MonadUnliftIO m
                => ParConduit i1 o m ()
                -> ParConduit i2 o m ()
                -> ParConduit (i1, i2) o m ()

    mapInput :: forall i1 i2 o m a .
                (i2 -> i1)
                -> ParConduit i1 o m a
                -> ParConduit i2 o m a

    mapOutput :: forall i o1 o2 m a .
                (o1 -> o2)
                -> ParConduit i o1 m a 
                -> ParConduit i o2 m a

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
-} 
