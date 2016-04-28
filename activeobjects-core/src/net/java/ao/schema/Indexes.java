package net.java.ao.schema;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines database indexes for an entity.
 *
 * <p>
 *     The {@link #value} should contain an array of {@link Index} declarations.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexes {
    Index[] value();
}