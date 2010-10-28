package net.java.ao;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;

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
     * Gets the table name converter to be used with the (to be) configured {@link net.java.ao.EntityManager}.
     *
     * @return a non-{@code null} {@link TableNameConverter table name converter}
     */
    TableNameConverter getTableNameConverter();

    /**
     * Gets the field name converter to be used with the (to be) configured {@link net.java.ao.EntityManager}
     *
     * @return a non-{@code null} {@link net.java.ao.schema.FieldNameConverter field name converter}
     */
    FieldNameConverter getFieldNameConverter();

    /**
     * Gets the schema configuration to be used with the (to be) configured {@link net.java.ao.EntityManager}
     *
     * @return a non-{@code null} {@link net.java.ao.SchemaConfiguration schema configuration}
     */
    SchemaConfiguration getSchemaConfiguration();
}
