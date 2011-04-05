package net.java.ao.schema;

import java.lang.reflect.Method;

public final class IsAFieldNameResolver extends AbstractFieldNameResolver
{
    public IsAFieldNameResolver()
    {
        super(true);
    }

    public boolean accept(Method method)
    {
        return method.getName().startsWith("is");
    }

    public String resolve(Method method)
    {
        return method.getName().substring(2);
    }
}
