package net.java.ao.schema;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines a database index.
 *
 * <p>
 *     The index can be either simple or composite.
 *     Please note that the actual name of the index may differ. This field is case <strong>insensitive</strong>.
 * </p>
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Index {

    /**
     * @return the table unique index name.
     */
    String name();

    /**
     * @return an array of accessors or mutators to the columns that should be included in the index.
     */
    String[] methodNames();
}
