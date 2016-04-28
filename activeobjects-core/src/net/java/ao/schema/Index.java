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
 *     The {@link #value()} contains an array of accessors to the fields that should be included in the index.
 * </p>
 *
 * @author Daniel Spiewak
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Index {
    String[] value() default {};
}
