package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

/**
 * A factory which creates {@link SimpleTableInfoResolver SimpleTableInfoResolvers}
 * @see SimpleTableInfoResolver
 */
public class SimpleTableInfoResolverFactory implements TableInfoResolverFactory
{
    @Override
    public TableInfoResolver create(NameConverters nameConverters, TypeManager typeManager)
    {
        return new SimpleTableInfoResolver(nameConverters, typeManager);
    }
}
