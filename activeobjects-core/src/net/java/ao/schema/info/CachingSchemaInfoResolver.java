package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachingSchemaInfoResolver extends SchemaInfoResolverWrapper implements SchemaInfoResolver
{

    private final ConcurrentMap<CacheKey, SchemaInfo> cache = new ConcurrentHashMap<CacheKey, SchemaInfo>();

    public CachingSchemaInfoResolver()
    {
        super(new DefaultSchemaInfoResolver());
    }

    public CachingSchemaInfoResolver(SchemaInfoResolver delegate)
    {
        super(delegate);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RawEntity<?>> SchemaInfo<T> resolve(NameConverters nameConverters, Class<T> type)
    {
        CacheKey key = new CacheKey(nameConverters, type);
        SchemaInfo<T> schemaInfo = cache.get(key);
        if (schemaInfo == null)
        {
            SchemaInfo<T> created = super.resolve(nameConverters, type);
            schemaInfo = cache.putIfAbsent(key, created);
            if (schemaInfo == null)
            {
                schemaInfo = created;
            }
        }
        return schemaInfo;
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
