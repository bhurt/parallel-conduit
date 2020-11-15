{-# LANGUAGE GADTs               #-}
{-# LANGUAGE Safe                #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-|

Module          : Data.Conduit.Parallel.Internal.Duct
Copyright       : (c) Brian Hurt, 2020
License         : BSD 3-Clause
Maintainer      : Brian Hurt <bhurt42@gmail.com>

This module implements ducts: a wrapper around classic MVars giving them
unix-pipe-like semantics.

= The Problem

We want to use MVars to pass values between the different stages of a
parallel conduit pipeline.  MVars give us two huge advantages.  Firstly,
they provide back-pressure, so that producers do not get too far ahead
of consumers, meaning that memory use is bounded.  Second, they avoid
the "thundering herd" problem- when a write happens on an MVar that has
multiple readers on it, one and only one of the readers is woken up,
the rest remain blocked.

The problem with using plain MVars is that there is no way for the
consumer side to signal that it has stopped consuming items to the
producer side.  Consider the case where the producer wants to produce
a billion elements, but after consuming the first 10, the consumer
quits.  Should the producer continue producing the remaining 999,999,990
elements?

= The Solution

What we want is a semantics similar to that of a unix pipe.  You have
a write end, a read end, and most importantly, the ability for either
end to close the channel.

-}
module Data.Conduit.Parallel.Internal.Duct(
    IsClosed(..),
    ReadDuct,
    readDuct,
    closeReadDuct,
    WriteDuct,
    comapWriteDuct,
    ductWrite,
    closeWriteDuct,
    createDuct,
    closedReadDuct,
    closedWriteDuct
) where

    import           Control.Concurrent.MVar
    import           Control.Concurrent.STM
    import           Control.DeepSeq
    import           Control.Exception       (evaluate)
    import           Control.Monad

    data IsClosed =
        IsClosed
        | IsNotClosed

    data DuctState =
        Open !Int
        | ClosedForWrites !Int  -- We are closed for writing, but have
                                -- outstanding writes so reads are still OK.
        | Closed

    data Duct a = Duct {
        ductState :: TVar DuctState,
        ductMVar :: MVar (Maybe a)
    }

    data ReadDuct a where
        ReadClosed   :: ReadDuct a
        ReadUnmapped :: (Duct a) -> ReadDuct a
        ReadMapped   :: forall b a . (Duct b) -> (b -> a) -> ReadDuct a

    instance Functor ReadDuct where
        fmap _ ReadClosed       = ReadClosed
        fmap f (ReadUnmapped d) = ReadMapped d f
        fmap f (ReadMapped d g) = ReadMapped d (f . g)

    readDuct :: ReadDuct a -> IO (Maybe a)
    readDuct ReadClosed          = return Nothing
    readDuct (ReadUnmapped duct) = doDuctRead duct
    readDuct (ReadMapped duct f) = fmap f <$> doDuctRead duct

    doDuctRead :: forall a . Duct a -> IO (Maybe a)
    doDuctRead duct = join $ atomically $ readSTM
        where
            readSTM :: STM (IO (Maybe a))
            readSTM = do
                r <- readTVar (ductState duct)
                case r of
                    Open n -> do
                        writeTVar (ductState duct) (Open (n - 1))
                        return $ takeMVar (ductMVar duct)
                    ClosedForWrites n -> do
                        let newstate = if (n > 1)
                                        then ClosedForWrites (n-1)
                                        else Closed
                        writeTVar (ductState duct) newstate
                        return $ takeMVar (ductMVar duct)
                    Closed -> return $ return Nothing

    closeReadDuct :: ReadDuct a -> IO ()
    closeReadDuct ReadClosed          = return ()
    closeReadDuct (ReadUnmapped duct) = doCloseReadDuct duct
    closeReadDuct (ReadMapped duct _) = doCloseReadDuct duct

    doCloseReadDuct :: Duct a -> IO ()
    doCloseReadDuct duct = join $ atomically $ closeSTM
        where
            closeSTM :: STM (IO ())
            closeSTM = do
                r <- readTVar (ductState duct)
                case r of
                    Closed -> return $ return ()
                    ClosedForWrites n -> do
                        writeTVar (ductState duct) Closed
                        return $ terminal n
                    Open n -> do
                        writeTVar (ductState duct) Closed
                        return $ terminal n

            terminal :: Int -> IO ()
            terminal n
                | n > 0 = do
                    _ <- takeMVar (ductMVar duct)
                    terminal (n-1)
                | n < 0 = do
                    putMVar (ductMVar duct) Nothing
                    terminal (n+1)
                | otherwise = return ()

    data WriteDuct a where
        WriteClosed   :: WriteDuct a
        WriteUnmapped :: (Duct a) -> WriteDuct a
        WriteMapped   :: forall b a . NFData b
                            => (a -> b)
                            -> Duct b
                            -> WriteDuct a

    -- I can't use contravariant functors here, due to the need for
    -- an NFData constraint.
    comapWriteDuct :: NFData b => (a -> b) -> WriteDuct b -> WriteDuct a
    comapWriteDuct _ WriteClosed       = WriteClosed
    comapWriteDuct f (WriteUnmapped d) = WriteMapped f d
    comapWriteDuct f (WriteMapped g d) = WriteMapped (g . f) d

    ductWrite :: forall a . NFData a => WriteDuct a -> a -> IO IsClosed
    ductWrite WriteClosed       _ = return IsClosed
    ductWrite (WriteUnmapped d) a = doDuctWrite d a
    ductWrite (WriteMapped f d) a = doDuctWrite d (f a)

    doDuctWrite :: forall a . NFData a => Duct a -> a -> IO IsClosed
    doDuctWrite duct a = do
            val <- evaluate (force (Just a))
            join $ atomically $ writeSTM val
        where
            writeSTM :: Maybe a -> STM (IO IsClosed)
            writeSTM val = do
                r <- readTVar (ductState duct)
                case r of
                    Open n -> do
                        writeTVar (ductState duct) (Open (n + 1))
                        return $ writeIO val
                    ClosedForWrites _ -> return $ return IsClosed
                    Closed -> return $ return IsClosed

            writeIO :: Maybe a -> IO IsClosed
            writeIO val = do
                putMVar (ductMVar duct) val
                s <- readTVarIO (ductState duct)
                case s of
                    Open _        -> return IsNotClosed
                    ClosedForWrites _ -> return IsClosed
                    Closed        -> return IsClosed

    closeWriteDuct :: WriteDuct a -> IO ()
    closeWriteDuct WriteClosed       = return ()
    closeWriteDuct (WriteUnmapped d) = doCloseWriteDuct d
    closeWriteDuct (WriteMapped _ d) = doCloseWriteDuct d

    doCloseWriteDuct :: Duct a -> IO ()
    doCloseWriteDuct duct = join $ atomically $ closeSTM
        where
            closeSTM :: STM (IO ())
            closeSTM = do
                r <- readTVar (ductState duct)
                case r of
                    Open n
                        | n > 0 -> do
                            writeTVar (ductState duct) (ClosedForWrites n)
                            return $ return ()
                        | n < 0 -> do
                            writeTVar (ductState duct) Closed
                            return $ terminal n
                        | otherwise -> do
                            writeTVar (ductState duct) Closed
                            return $ return ()
                    ClosedForWrites _ -> return $ return ()
                    Closed -> return $ return ()

            terminal :: Int -> IO ()
            terminal n =
                if (n < 0)
                then do
                    putMVar (ductMVar duct) Nothing
                    terminal (n+1)
                else return ()
                
    createDuct :: forall a . IO (ReadDuct a, WriteDuct a)
    createDuct = do
        tvar <- newTVarIO (Open 0)
        mvar <- newEmptyMVar
        let duct = Duct {
                    ductState = tvar,
                    ductMVar = mvar }
        return (ReadUnmapped duct, WriteUnmapped duct)

    closedReadDuct :: forall a . ReadDuct a
    closedReadDuct = ReadClosed

    closedWriteDuct :: forall a . WriteDuct a
    closedWriteDuct = WriteClosed

