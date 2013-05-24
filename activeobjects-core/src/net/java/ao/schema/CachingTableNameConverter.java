package net.java.ao.schema;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import net.java.ao.RawEntity;

import java.util.Map;

import static com.google.common.base.Preconditions.*;

/**
 * <p>A table name converter that simply caches the converted table names.</p>
 * <p>This implementation uses a {@link java.util.concurrent.ConcurrentMap} and is thread safe.</p>
 *
 * @since 0.9
 */
public class CachingTableNameConverter implements TableNameConverter
{
    private final Map<Class<? extends RawEntity<?>>, String> cache;

    public CachingTableNameConverter(final TableNameConverter delegateTableNameConverter)
    {
        checkNotNull(delegateTableNameConverter);
        this.cache = new MapMaker().makeComputingMap(new Function<Class<? extends RawEntity<?>>, String>()
        {
            public String apply(Class<? extends RawEntity<?>> entityClass)
            {
                return delegateTableNameConverter.getName(entityClass);
            }
        });
    }

    public String getName(Class<? extends RawEntity<?>> entityClass)
    {
        return cache.get(entityClass);
    }
}
