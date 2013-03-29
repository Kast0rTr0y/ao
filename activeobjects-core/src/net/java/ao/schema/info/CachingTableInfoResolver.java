package net.java.ao.schema.info;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

public class CachingTableInfoResolver extends TableInfoResolverWrapper implements TableInfoResolver
{

    private final Cache<Class<? extends RawEntity<?>>, TableInfo> cache;

    public CachingTableInfoResolver(TableInfoResolver delegate)
    {
        super(delegate);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends RawEntity<?>>, TableInfo>() {
            @SuppressWarnings("unchecked")
            @Override
            public TableInfo load(Class type) throws Exception {
                return CachingTableInfoResolver.super.resolve(type);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RawEntity<K>, K> TableInfo<T, K> resolve(Class<T> type)
    {
        return cache.getUnchecked(type);
    }

}
