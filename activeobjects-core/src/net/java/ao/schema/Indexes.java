package net.java.ao.schema;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Defines database indexes for an entity.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexes {
    Index[] value();
}