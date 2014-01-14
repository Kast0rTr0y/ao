package net.java.ao;

import net.java.ao.schema.NameConverters;
import net.java.ao.schema.info.EntityInfoResolverFactory;

/**
 * This represents a configuration for entity manager creation.
 */
public interface EntityManagerConfiguration
{
    /**
     * @deprecated since 0.25. EntityManager now no longer caches.
     */
    @Deprecated
    boolean useWeakCache();

    /**
     * Gets the name converters to be used with the (to be) configured {@link net.java.ao.EntityManager}.
     *
     * @return a non-{@code null} {@link NameConverters name converter}
     */
    NameConverters getNameConverters();

    /**
     * Gets the schema configuration to be used with the (to be) configured {@link net.java.ao.EntityManager}
     *
     * @return a non-{@code null} {@link net.java.ao.SchemaConfiguration schema configuration}
     */
    SchemaConfiguration getSchemaConfiguration();

    /**
     * Gets a EntityInfo factory to use when parsing the schema configuration annotated on
     * the {@link RawEntity entity} type
     * @return a non-{@code null} {@link net.java.ao.schema.info.EntityInfoResolver EntityInfo factory}
     */
    EntityInfoResolverFactory getEntityInfoResolverFactory();

}
