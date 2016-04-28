package net.java.ao.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a database index.
 *
 * <p>
 *     The index can be either simple or composite.
 *     The {@link #methods()} contains an array of accessors or mutators to the columns that should be included in the index.
 *     The {@link #name()} should specify table unique index name.
 *     Please note that the actual name of the index may differ.This field is case <strong>insensitive</strong>.
 * </p>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Index {
    String name();
    String[] methodNames();
}
