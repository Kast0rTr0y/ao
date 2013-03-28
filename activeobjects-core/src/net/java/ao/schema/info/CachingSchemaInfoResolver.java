package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachingSchemaInfoResolver extends SchemaInfoResolverWrapper implements SchemaInfoResolver
{

    private final ConcurrentMap<Class, SchemaInfo> cache = new ConcurrentHashMap<Class, SchemaInfo>();

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
        SchemaInfo<T> schemaInfo = cache.get(type);
        if (schemaInfo == null)
        {
            SchemaInfo<T> created = super.resolve(nameConverters, type);
            schemaInfo = cache.putIfAbsent(type, created);
            if (schemaInfo == null)
            {
                schemaInfo = created;
            }
        }
        return schemaInfo;
    }

}
