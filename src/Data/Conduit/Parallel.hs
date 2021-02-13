{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-|
Module      : Data.Conduit.Parallel
Description : Parallel conduits, using Async, MVars, and UnliftIO
Copyright   : (c) Brian Hurt, 2020
License     : BSD 3-clause
Maintainer  : bhurt42@gmail.com
Stability   : experimental

This library implements a parallel version of the 
[Data.Conduit](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html)
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
    concurrently,
    tee,
    merge,
    consumeAll,
    cache,
    mapInput,
    mapOutput,

    -- * Alternate ParConduit constructors
    --
    -- | Other ways to contruct single-segment ParConduits.
    basic,
    simple,

    -- * Lazy datatype
    --
    -- | For sidestepping the NFData requirement
    --
    Lazy(..)

) where

    import           Control.DeepSeq
    import           Control.Monad
    import           Control.Monad.STM                   (retry)
    import           Control.Monad.Trans.Cont
    import qualified Data.Atomics.Counter                as Atomics
    import           Data.Conduit                        (Void)
    import qualified Data.Conduit                        as C
    import           Data.Conduit.Parallel.Internal.Duct
    import           Data.Sequence                       (Seq)
    import qualified Data.Sequence                       as Seq
    import           UnliftIO                            hiding (concurrently)

    -- A comment on the image tags: I don't know how to include the image
    -- files as part of the documentation.  So instead, I (ab)use github
    -- as an image file server.  This will probably get me into trouble
    -- sooner or later.

    -- | A ParConduit segment.
    --
    -- The type is modeled off the [Data.Conduit.ConduitT](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#g:2) type.  We have
    -- four type variables:
    --
    --  * @i@ is the type of values being consumed, the "input" type
    --
    --  * @o@ is the type of values being produced, the "output" type
    --
    --  * @m@ is the monad being executed in
    --
    --  * @r@ is the result type, produced when the segment exits.
    --
    -- We might represent a segment pictorially like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/example.png example>>
    --
    -- ParConduit instances where the @i@ type is @()@ are called
    -- sources.  They only produce output, they don't consume input.
    -- Instances where the @o@ type is @Void@ are called sinks.
    -- They only consume input, and don't produce output.
    --
    -- We do not supply Applicative or Monad instances for this
    -- type- the implementation is difficult and the semantics would
    -- be complicated and surprising.  If you want to do this, do
    -- it with a normal Conduit, and then convert the Conduit into
    -- a ParConduit.
    --

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

    -- | Runs a ParConduit.
    --
    -- Directly analogous to [Data.Conduit.runConduit](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#v:runConduit).
    runParConduit :: forall m r . MonadUnliftIO m
                        => ParConduit () Void m r -> m r
    runParConduit pc = runContT
                        (getParConduit pc closedReadDuct closedWriteDuct) id

    basic :: forall i o m r .
                (MonadUnliftIO m
                , NFData o)
                => ((m (Maybe i)) -> (o -> m Bool) -> m r)
                -> ParConduit i o m r
    basic f = justSpawn go
        where
            go :: ReadDuct i -> WriteDuct o -> m r
            go rd wd = do
                r <- f (wrapRead rd) (wrapWrite wd)
                liftIO $ do
                    closeReadDuct rd
                    closeWriteDuct wd
                return r

            wrapRead :: ReadDuct i -> m (Maybe i)
            wrapRead = liftIO . ductRead

            wrapWrite :: WriteDuct o -> o -> m Bool
            wrapWrite wd o = do
                ic <- liftIO $ ductWrite wd o
                return $ case ic of
                            IsClosed -> False
                            IsNotClosed -> True

    -- | Lift a normal Conduit segment into a ParConduit segment.
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
                r <- liftIO $ ductRead rd
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

    simple :: forall i o m .
                (MonadUnliftIO m
                , NFData o)
                => (i -> m o)
                -> ParConduit i o m ()
    simple f = justSpawn go
        where
            go :: ReadDuct i -> WriteDuct o -> m ()
            go rd wd = do
                mi <- liftIO $ ductRead rd
                case mi of
                    Nothing -> liftIO $ closeWriteDuct wd
                    Just i -> do
                        o <- f i
                        isc <- liftIO $ ductWrite wd o
                        case isc of
                            IsClosed -> liftIO $ closeReadDuct rd
                            IsNotClosed -> go rd wd

    {-
    complex :: forall i o m r s .
                (MonadUnliftIO m
                , NFData o)
                => (s -> i -> m (Either s (s, [o])))
                -> s
                -> (s -> m r)
                -> ParConduit i o m r
    complex f sinit done = justSpawn (go sinit)
        where
            go :: s -> ReadDuct i -> WriteDuct o -> m r
            go s rd wd = do
                mi <- liftIO $ ductRead rd
                case mi of
                    Nothing -> do
                        liftIO $ closeWriteDuct wd
                        done s
                    Just i -> do
                        res <- f s i
                        case res of
                            Left s2 -> do
                                liftIO $ closeReadDuct rd
                                liftIO $ closeWriteDuct wd
                                done s2
                            Right (s2, os) -> writeOs rd wd s2 os

            writeOs :: ReadDuct i -> WriteDuct o -> s -> [ o ] -> m r
            writeOs rd wd s [] = go s rd wd
            writeOs rd wd s (x:xs) = do
                ic <- liftIO $ ductWrite wd x
                case ic of
                    IsClosed -> do
                        liftIO $ closeReadDuct rd
                        done s

    fold :: forall i m r s .
            (MonadUnliftIO m)
            => (s -> i -> m (Either s s))
            -> s
            -> (s -> m r)
            -> ParConduit i Void m r

    unfold :: forall o m r s .
                (MonadUnliftIO m
                , NFData o)
                => (s -> m (Either s (s, [o])))
                -> s
                -> (s -> m r)
                -> ParConduit () o m r
    -}

    -- | Shared code for all the parFuse* functions.
    parFuseInternal :: forall i o x m r1 r2 r .
                    MonadIO m
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

    -- | Fuse two conduits into one with the result from the latter.
    --
    -- Directly analogous to [Data.Conduit.fuse](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#v:fuse).
    --
    -- The output of the first segment becomes the input to the second.
    -- The result of the first segment is discarded (and thus should be unit)
    -- while the result of the second segment is the result of the fused
    -- segment.  Visually, the code:
    --
    -- > parFuse par1 par2
    --
    -- is wired up like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/parFuse.png parFuse>>
    --
    parFuse :: forall i o x m r .
                    MonadIO m
                    => ParConduit i x m ()
                    -> ParConduit x o m r
                    -> ParConduit i o m r
    parFuse = parFuseInternal (flip const)

    -- | Fuse two conduits into one with the result from the former.
    --
    -- Directly analogous to [Data.Conduit.fuseUpstream](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#v:fuseUpstream).
    --
    -- The output of the first segment becomes the input to the second.
    -- The result of the second segment is discarded (and thus should be unit)
    -- while the result of the first segment is the result of the fused
    -- segment.  Visually, the code:
    --
    -- > parFuseUpstream par1 par2
    --
    -- is wired up like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/parFuseUpstream.png parFuseUpstream>>
    --
    parFuseUpstream :: forall i o x m r .
                    MonadIO m
                    => ParConduit i x m r
                    -> ParConduit x o m ()
                    -> ParConduit i o m r
    parFuseUpstream = parFuseInternal const

    -- | Fuse two conduits into one with the result the tuple of the two
    --   results.
    --
    -- Directly analogous to [Data.Conduit.fuseBoth](https://hackage.haskell.org/package/conduit-1.3.4/docs/Data-Conduit.html#v:fuseBoth).
    --
    -- The output of the first segment becomes the input to the second.
    -- The result is the tuple of the two results.
    -- Visually, the code:
    --
    -- > parFuseBoth par1 par2
    --
    -- is wired up like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/parFuseBoth.png parFuseBoth>>
    --
    parFuseBoth :: forall i o x m r1 r2 .
                    MonadIO m
                    => ParConduit i x m r1
                    -> ParConduit x o m r2
                    -> ParConduit i o m (r1, r2)
    parFuseBoth = parFuseInternal (\x y -> (x,y))

    -- | Fuse two conduits into one with the result the concatentation
    --   of the two results.
    --
    -- The output of the first segment becomes the input to the second.
    -- The result is the concatenation via the semigroup append of the
    -- two results.  The result types therefor need to implement
    -- Semigroup.
    --
    -- Visually, the code:
    --
    -- > parFuseS par1 par2
    --
    -- is wired up like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/parFuseS.png parFuseS>>
    --
    parFuseS :: forall i o x m r .
                    (MonadIO m, Semigroup r)
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
            r <- liftIO $ ductRead rd
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

    -- | Run two segments concurrently.
    --
    -- Given two segments, produce a single segment where the input
    -- is duplicated and sent to both segments, and the output is
    -- merged.  The result is the result of the second segment-
    -- the result of the first segment is discarded (and thus must
    -- be unit).
    --
    -- Both threads will put their results into the same MVar- so
    -- the results will be more or less randomly intermingled.  One
    -- pattern is then to have both segments be sinks, and not actually
    -- output anything.  If output from only one of the two segments
    -- is needed, consider using the `tee` function instead.
    --
    -- Visually, the code:
    --
    -- > concurrently par1 par2
    --
    -- is wired up internally like:
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/concurrently.png concurrently>>
    --
    concurrently :: forall m i o r .
                    (MonadUnliftIO m,
                    NFData i)
                    => ParConduit i o m ()
                    -> ParConduit i o m r
                    -> ParConduit i o m r
    concurrently path1 path2 = ParConduit go
        where
            go :: forall s . ReadDuct i -> WriteDuct o -> ContT s m (m r)
            go rd wd = do
                (rc1, wc1) <- liftIO $ createDuct
                (rc2, wc2) <- liftIO $ createDuct
                m1 <- getParConduit path1 rc1 wd
                m2 <- getParConduit path2 rc2 wd
                m3 <- spawn $ copyWithClose rd [ wc1, wc2 ]
                return $ m3 >> m1 >> m2

    -- | Copy the stream of inputs to a sink
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/tee.png tee>>
    -- 
    tee :: forall i m r .
            (MonadUnliftIO m
            , NFData i)
            => ParConduit i Void m r
            -> ParConduit i i m r
    tee sink = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m r)
            go rd wd = do
                (ird, iwd) <- liftIO $ createDuct
                a1 :: m r <- getParConduit sink ird closedWriteDuct
                a2 :: m () <- spawn $ copyWithClose rd [ wd, iwd ]
                return $ const <$> a1 <*> a2

    -- | Add a source's output to the stream.
    --
    -- <<https://raw.githubusercontent.com/bhurt/parallel-conduit/master/docs/merge.png merge>>
    -- 
    merge :: forall i m r .
            (MonadUnliftIO m
            , NFData i)
            => ParConduit () i m r
            -> ParConduit i i m r
    merge src = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m r)
            go ord owd = do
                doneCounter :: Atomics.AtomicCounter
                    <- liftIO $ Atomics.newCounter 2
                m1 :: m () <- spawn $ doCopy ord owd doneCounter
                (ird, iwd) <- liftIO $ createDuct
                m2 :: m r <- getParConduit src closedReadDuct iwd
                m3 :: m () <- spawn $ doCopy ird owd doneCounter
                return $ do
                    m1 
                    r <- m2
                    m3
                    return r

            doCopy :: ReadDuct i -> WriteDuct i -> Atomics.AtomicCounter -> m ()
            doCopy rd wd acnt = do
                copier rd [ wd ]
                liftIO $ closeReadDuct rd
                n <- liftIO $ Atomics.incrCounter (negate 1) acnt
                when (n == 0) $ liftIO $ closeWriteDuct wd

    consumeAll :: forall m i .
                    (MonadUnliftIO m
                    , NFData i)
                    => ParConduit i i m ()
    consumeAll = ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m ())
            go rd wd = spawn $ doCopy rd wd

            doCopy :: ReadDuct i -> WriteDuct i -> m ()
            doCopy rd wd = do
                r <- liftIO $ ductRead rd
                case r of
                    Nothing -> do
                        liftIO $ closeWriteDuct wd
                        return ()
                    Just x -> do
                        ic <- liftIO $ ductWrite wd x
                        case ic of
                            IsClosed -> loop rd
                            IsNotClosed -> doCopy rd wd

            loop :: ReadDuct i -> m ()
            loop rd = do
                r <- liftIO $ ductRead rd
                case r of
                    Nothing -> return ()
                    Just _ -> loop rd

    cache :: forall m i .
                (MonadUnliftIO m
                , NFData i)
                => Int
                -> ParConduit i i m ()
    cache csize = if (csize < 1)
                    then error $ "ParConduit.cache: cache size of "
                                    ++ show csize
                                    ++ " is invalid."
                    else ParConduit go
        where
            go :: forall s .  ReadDuct i -> WriteDuct i -> ContT s m (m ())
            go rd wd = do
                closedTVar <- liftIO $ newTVarIO IsNotClosed
                seqTVar <- liftIO $ newTVarIO Seq.empty
                m1 <- spawn $ readLoop closedTVar seqTVar rd
                m2 <- spawn $ writeLoop closedTVar seqTVar wd
                return $ m1 >> m2

            readLoop :: TVar IsClosed
                        -> TVar (Seq i)
                        -> ReadDuct i
                        -> m ()
            readLoop closedTVar seqTVar rd = do
                r <- liftIO $ ductRead rd
                case r of
                    Nothing -> atomically $ writeTVar closedTVar IsClosed
                    Just i -> do
                        ic <- atomically $ do
                                ic <- readTVar closedTVar
                                case ic of
                                    IsClosed    -> return ()
                                    IsNotClosed -> do
                                        sq <- readTVar seqTVar
                                        if Seq.length sq >= csize
                                        then retry
                                        else writeTVar seqTVar (sq Seq.|> i)
                                return ic
                        case ic of
                            IsClosed    -> liftIO $ closeReadDuct rd
                            IsNotClosed -> readLoop closedTVar seqTVar rd

            writeLoop :: TVar IsClosed
                            -> TVar (Seq i)
                            -> WriteDuct i
                            -> m ()
            writeLoop closedTVar seqTVar wd =
                let loop x = do
                                ic <- liftIO $ ductWrite wd x
                                case ic of
                                    IsNotClosed ->
                                        writeLoop closedTVar seqTVar wd
                                    IsClosed    ->
                                        atomically $ do
                                            writeTVar closedTVar IsClosed
                                            writeTVar seqTVar Seq.empty
                                            return ()
                in
                join . atomically $ do
                        sq <- readTVar seqTVar
                        case Seq.viewl sq of
                            (i Seq.:< sq2) -> do
                                writeTVar seqTVar sq2
                                return $ loop i
                            Seq.EmptyL     -> do
                                ic <- readTVar closedTVar 
                                case ic of
                                    IsNotClosed -> retry
                                    IsClosed    -> 
                                        return $ liftIO $ closeWriteDuct wd

{-

    fork :: forall i o m r.
            (MonadUnliftIO m
            , Monoid r)
            => ParConduit i Void m r
            -> ParConduit i Void m r
            -> ParConduit i Void m r

    combine :: forall i o m r
                (MonadUnliftIO m
                , Monoid r)
                => ParConduit () o m r
                -> ParConduit () o m r
                -> ParConduit () o m r

    replicate :: forall i o m .
            MonadUnliftIO m
            => Int
            -> ParConduit i o m ()
            -> ParConduit i o m ()

    synchronous :: forall a b m . MonadUnliftIO m
                => Int
                -> (a -> m [b])
                -> ParConduit a b m ()


    fan :: forall i1 i2 o m . MonadUnliftIO m
            => ParConduit i1 o m ()
            -> ParConduit i2 o m ()
            -> ParConduit (Either i1 i2) o m ()

    partition :: forall i1 i2 o m . MonadUnliftIO m
                => ParConduit i1 o m ()
                -> ParConduit i2 o m ()
                -> ParConduit (i1, i2) o m ()

-}

    mapInput :: forall i1 i2 o m a .
                (i2 -> i1)
                -> ParConduit i1 o m a
                -> ParConduit i2 o m a
    mapInput f par = ParConduit go
        where
            go :: forall s .  ReadDuct i2 -> WriteDuct o -> ContT s m (m a)
            go rduct = getParConduit par (fmap f rduct)

    mapOutput :: forall i o1 o2 m a .
                NFData o2
                => (o1 -> o2)
                -> ParConduit i o1 m a
                -> ParConduit i o2 m a
    mapOutput f par = ParConduit go
        where
            go :: forall s . ReadDuct i -> WriteDuct o2 -> ContT s m (m a)
            go rduct wduct = getParConduit par rduct (comapWriteDuct f wduct)


    newtype Lazy a = Lazy { getLazy :: a }

    instance NFData (Lazy a) where
        rnf = rwhnf . getLazy
