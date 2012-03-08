package net.java.ao.cache;

import net.java.ao.RawEntity;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.*;

final class ConcurrentRelationsCache implements RelationsCache
{
    private final RelationsCache cache;
    private final ReadWriteLock lock;

    ConcurrentRelationsCache(RelationsCache cache)
    {
        this.cache = checkNotNull(cache);
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public <T extends RawEntity<K>, K> T[] get(final RawEntity<?> from, final Class<T> toType, final Class<? extends RawEntity<?>> throughType,
                                               final String[] fields, final String where)
    {
        return withLock(lock.readLock(), new Callable<T[]>()
        {
            @Override
            public T[] call()
            {
                return cache.get(from, toType, throughType, fields, where);
            }
        });
    }

    @Override
    public void put(final RawEntity<?> from, final RawEntity<?>[] through, final Class<? extends RawEntity<?>> throughType,
                    final RawEntity<?>[] to, final Class<? extends RawEntity<?>> toType, final String[] fields, final String where)
    {
        withLock(lock.writeLock(), new Callable<Void>()
        {
            @Override
            public Void call()
            {
                cache.put(from, through, throughType, to, toType, fields, where);
                return null;
            }
        });
    }

    @Override
    public void remove(final Class<? extends RawEntity<?>>... types)
    {
        withLock(lock.writeLock(), new Callable<Void>()
        {
            @Override
            public Void call()
            {
                cache.remove(types);
                return null;
            }
        });
    }

    @Override
    public void remove(final RawEntity<?> entity, final String[] fields)
    {
        withLock(lock.writeLock(), new Callable<Void>()
        {
            @Override
            public Void call()
            {
                cache.remove(entity, fields);
                return null;
            }
        });
    }

    @Override
    public void flush()
    {
        withLock(lock.writeLock(), new Callable<Void>()
        {
            @Override
            public Void call()
            {
                cache.flush();
                return null;
            }
        });
    }

    private static <T> T withLock(Lock l, Callable<T> callable)
    {
        l.lock();
        try
        {
            return callable.call();
        }
        finally
        {
            l.unlock();
        }
    }

    private static interface Callable<T>
    {
        T call();
    }
}
