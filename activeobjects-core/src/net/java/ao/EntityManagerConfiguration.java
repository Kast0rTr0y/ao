package net.java.ao;

import net.java.ao.schema.info.SchemaInfoResolver;
import net.java.ao.schema.NameConverters;

/**
 * This represents a configuration for entity manager creation.
 */
public interface EntityManagerConfiguration
{
    /**
     * Whether or not the {@link net.java.ao.EntityManager} should use weak references for caching
     *
     * @return {@code true} if the entity manager should use weak references for caching.
     */
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
     * Gets a SchemaInfo factory to use when parsing the schema configuration annotated on
     * the {@link RawEntity entity} type
     * @return a non-{@code null} {@link net.java.ao.schema.info.SchemaInfoResolver SchemaInfo factory}
     */
    SchemaInfoResolver getSchemaInfoResolver();

}
