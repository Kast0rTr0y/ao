package net.java.ao.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Explicitly specifies the maximum length, in characters, of the underlying
 * database column corresponding to the method in question.  This is valid for
 * accessors of type String, and is ignored for all other types.  The value can
 * be a positive integer or {@link #UNLIMITED}; an unlimited-length string is
 * stored as the corresponding "long string" database type (TEXT, CLOB, etc.
 * depending on the database provider).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface StringLength {

    /**
     * Specifies the maximum length of the database column in characters.
     * This can be a positive integer or {@link #UNLIMITED}.
     */
    int value();

    public static final int MAX_LENGTH = 767;

    public static final int UNLIMITED = -1;
}
