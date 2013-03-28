package net.java.ao.schema.info;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

public class CachingSchemaInfoResolver extends SchemaInfoResolverWrapper implements SchemaInfoResolver
{

    private final Cache<CacheKey, SchemaInfo> cache;

    public CachingSchemaInfoResolver()
    {
        this(new DefaultSchemaInfoResolver());
    }

    public CachingSchemaInfoResolver(SchemaInfoResolver delegate)
    {
        super(delegate);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<CacheKey, SchemaInfo>() {
            @SuppressWarnings("unchecked")
            @Override
            public SchemaInfo load(CacheKey key) throws Exception {
                return CachingSchemaInfoResolver.super.resolve(key.nameConverters, key.type);
            }
        });
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RawEntity<?>> SchemaInfo<T> resolve(NameConverters nameConverters, Class<T> type)
    {
        return cache.getUnchecked(new CacheKey(nameConverters, type));
    }

    private static class CacheKey
    {
        private final NameConverters nameConverters;
        private final Class type;

        private CacheKey(NameConverters nameConverters, Class type)
        {
            this.nameConverters = nameConverters;
            this.type = type;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            return !(nameConverters != null ? !nameConverters.equals(cacheKey.nameConverters) : cacheKey.nameConverters != null)
                    && !(type != null ? !type.equals(cacheKey.type) : cacheKey.type != null);

        }

        @Override
        public int hashCode()
        {
            int result = nameConverters != null ? nameConverters.hashCode() : 0;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CacheKey{" +
                    "nameConverters=" + nameConverters +
                    ", type=" + type +
                    '}';
        }
    }

}
