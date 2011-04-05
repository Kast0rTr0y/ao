package net.java.ao.schema;

import java.lang.reflect.Method;

public final class GetterFieldNameResolver extends AbstractFieldNameResolver
{
    public GetterFieldNameResolver()
    {
        super(true);
    }

    public boolean accept(Method method)
    {
        return method.getName().startsWith("get");
    }

    public String resolve(Method method)
    {
        return method.getName().substring(3);
    }
}
