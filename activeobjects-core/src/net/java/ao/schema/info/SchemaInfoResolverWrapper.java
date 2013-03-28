package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

public abstract class SchemaInfoResolverWrapper implements SchemaInfoResolver
{

    private final SchemaInfoResolver delegate;

    protected SchemaInfoResolverWrapper(SchemaInfoResolver delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public <T extends RawEntity<?>> SchemaInfo<T> resolve(NameConverters nameConverters, Class<T> type)
    {
        return delegate.resolve(nameConverters, type);
    }
}
