package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

/**
 * A factory capable of currying the required managers for a {@link EntityInfoResolver} to be created
 *
 * @since 0.21
 */
public interface EntityInfoResolverFactory {

    /**
     * @param nameConverters the name converters
     * @param typeManager    the database type manager
     * @return a new {@link EntityInfoResolver}
     */
    EntityInfoResolver create(NameConverters nameConverters, TypeManager typeManager);

}
