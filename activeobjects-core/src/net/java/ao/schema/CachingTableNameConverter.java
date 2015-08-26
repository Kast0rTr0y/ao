package net.java.ao.schema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import net.java.ao.RawEntity;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>A table name converter that simply caches the converted table names.</p>
 * <p>This implementation uses a {@link com.google.common.cache.LoadingCache} and is thread safe.</p>
 *
 * @since 0.9
 */
public class CachingTableNameConverter implements TableNameConverter {
    private final LoadingCache<Class<? extends RawEntity<?>>, String> cache;

    public CachingTableNameConverter(final TableNameConverter delegateTableNameConverter) {
        checkNotNull(delegateTableNameConverter);
        this.cache = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends RawEntity<?>>, String>() {
            @Override
            public String load(final Class<? extends RawEntity<?>> key) throws Exception {
                return delegateTableNameConverter.getName(key);
            }
        });
    }

    public String getName(Class<? extends RawEntity<?>> entityClass) {
        return cache.getUnchecked(entityClass);
    }
}
