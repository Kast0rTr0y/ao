package net.java.ao.schema;

import java.lang.reflect.Method;

public final class PrimaryKeyFieldNameResolver extends AbstractFieldNameResolver
{
    public PrimaryKeyFieldNameResolver()
    {
        super(false);
    }

    @Override
    public boolean accept(Method method)
    {
        return method.isAnnotationPresent(PrimaryKey.class)
                && method.getAnnotation(PrimaryKey.class).value() != null
                && !method.getAnnotation(PrimaryKey.class).value().trim().equals("");
    }

    @Override
    public String resolve(Method method)
    {
        return method.getAnnotation(PrimaryKey.class).value();
    }
}
