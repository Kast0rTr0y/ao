package net.java.ao.schema.info;

import net.java.ao.RawEntity;

/**
 * An interface for any class with the ability to compute the {@link EntityInfo table information}
 * for an {@link RawEntity entity class}
 *
 * @since 0.21
 */
public interface EntityInfoResolver {

    /**
     * @param type the entity interface
     * @param <T>  the entity type
     * @param <K>  the type of the primary key for the entity
     * @return the {@link EntityInfo table information} for the entity
     */
    <T extends RawEntity<K>, K> EntityInfo<T, K> resolve(Class<T> type);

}
