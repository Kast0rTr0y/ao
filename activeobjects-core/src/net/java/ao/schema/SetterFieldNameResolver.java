package net.java.ao.schema;

import java.lang.reflect.Method;

public final class SetterFieldNameResolver extends AbstractFieldNameResolver
{
    public SetterFieldNameResolver()
    {
        super(true);
    }

    public boolean accept(Method method)
    {
        return method.getName().startsWith("set");
    }

    public String resolve(Method method)
    {
        return method.getName().substring(3);
    }
}
