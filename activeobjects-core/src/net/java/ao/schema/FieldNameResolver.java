package net.java.ao.schema;

import java.lang.reflect.Method;

/**
 * An interface to resolve field names.
 * Implementation should not transform (upper case, under score, camel case, etc...) the resolved name.
 */
public interface FieldNameResolver
{
    /**
     * Tells whether this field name resolver will be able to resolve a name from this method.
     *
     * @param method the method to figure out the field name for
     * @return {@code true} if {@link #resolve(Method)} will actually resolve the field name given the same method
     *         argument
     */
    boolean accept(Method method);

    /**
     * Resolves the field name for the method
     *
     * @param method the method to resolve the field name for.
     * @return the resolved field name. {@code null} is valid if no field name should be associated with this method.
     * @throws IllegalStateException if this method is called while {@link #accept(Method)} returns {@code false}
     */
    String resolve(Method method);

    /**
     * Tells whether the resolved method name from {@link #resolve(Method)} allows for further transformations.
     *
     * @return {@code true} if further transformations are allowed
     */
    boolean transform();
}
