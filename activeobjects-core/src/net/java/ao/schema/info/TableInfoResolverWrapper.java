package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

/**
 * A {@code TableInfoResolver} which delegates.
 */
public abstract class TableInfoResolverWrapper implements TableInfoResolver
{

    private final TableInfoResolver delegate;

    protected TableInfoResolverWrapper(TableInfoResolver delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public <T extends RawEntity<K>, K> TableInfo<T, K> resolve(Class<T> type)
    {
        return delegate.resolve(type);
    }
}
