package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

public class CachingTableInfoResolverFactory extends SimpleTableInfoResolverFactory {
    @Override
    public TableInfoResolver create(NameConverters nameConverters, TypeManager typeManager) {
        return new CachingTableInfoResolver(super.create(nameConverters, typeManager));
    }
}
