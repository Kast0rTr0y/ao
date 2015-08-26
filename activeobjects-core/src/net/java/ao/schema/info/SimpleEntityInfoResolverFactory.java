package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

/**
 * A factory which creates {@link SimpleEntityInfoResolver SimpleEntityInfoResolvers}
 *
 * @see SimpleEntityInfoResolver
 */
public class SimpleEntityInfoResolverFactory implements EntityInfoResolverFactory {
    @Override
    public EntityInfoResolver create(NameConverters nameConverters, TypeManager typeManager) {
        return new SimpleEntityInfoResolver(nameConverters, typeManager);
    }
}
