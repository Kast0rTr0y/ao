package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

/**
 * An interface for any class with the ability to compute the {@link TableInfo table information}
 * for an {@link RawEntity entity class}
 */
public interface TableInfoResolver
{

    /**
     * @param type the entity interface
     * @param <T> the entity type
     * @param <K> the type of the primary key for the entity
     * @return the {@link TableInfo table information} for the entity
     */
    <T extends RawEntity<K>, K> TableInfo<T, K> resolve(Class<T> type);

}
