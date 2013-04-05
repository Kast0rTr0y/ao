package net.java.ao.schema.info;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import net.java.ao.RawEntity;

/**
 * A {@code EntityInfoResolver} which caches the computed {@link EntityInfo} for any type it is given
 */
public class CachingEntityInfoResolver extends EntityInfoResolverWrapper implements EntityInfoResolver
{

    private final Cache<Class<? extends RawEntity<?>>, EntityInfo> cache;

    public CachingEntityInfoResolver(EntityInfoResolver delegate)
    {
        super(delegate);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends RawEntity<?>>, EntityInfo>() {
            @SuppressWarnings("unchecked")
            @Override
            public EntityInfo load(Class type) throws Exception {
                return CachingEntityInfoResolver.super.resolve(type);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RawEntity<K>, K> EntityInfo<T, K> resolve(Class<T> type)
    {
        return cache.getUnchecked(type);
    }

}
