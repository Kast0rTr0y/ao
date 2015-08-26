package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

/**
 * A factory which creates {@link CachingEntityInfoResolver CachingEntityInfoResolvers} which delegate to
 * {@link SimpleEntityInfoResolver SimpleEntityInfoResolvers}
 *
 * @see SimpleEntityInfoResolver
 * @see CachingEntityInfoResolver
 */
public class CachingEntityInfoResolverFactory extends SimpleEntityInfoResolverFactory {
    @Override
    public EntityInfoResolver create(NameConverters nameConverters, TypeManager typeManager) {
        return new CachingEntityInfoResolver(super.create(nameConverters, typeManager));
    }
}
