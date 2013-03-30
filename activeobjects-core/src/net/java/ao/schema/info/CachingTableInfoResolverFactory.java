package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

/**
 * A factory which creates {@link CachingTableInfoResolver CachingTableInfoResolvers} which delegate to
 * {@link SimpleTableInfoResolver SimpleTableInfoResolvers}
  * @see SimpleTableInfoResolver
  * @see CachingTableInfoResolver
 */
public class CachingTableInfoResolverFactory extends SimpleTableInfoResolverFactory {
    @Override
    public TableInfoResolver create(NameConverters nameConverters, TypeManager typeManager) {
        return new CachingTableInfoResolver(super.create(nameConverters, typeManager));
    }
}
