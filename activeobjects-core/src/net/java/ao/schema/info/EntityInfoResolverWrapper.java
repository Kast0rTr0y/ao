package net.java.ao.schema.info;

import net.java.ao.RawEntity;

/**
 * A {@code EntityInfoResolver} which delegates.
 */
public abstract class EntityInfoResolverWrapper implements EntityInfoResolver {

    private final EntityInfoResolver delegate;

    protected EntityInfoResolverWrapper(EntityInfoResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public <T extends RawEntity<K>, K> EntityInfo<T, K> resolve(Class<T> type) {
        return delegate.resolve(type);
    }
}
