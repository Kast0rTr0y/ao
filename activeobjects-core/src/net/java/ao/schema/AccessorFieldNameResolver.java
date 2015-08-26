package net.java.ao.schema;

import net.java.ao.Accessor;

import java.lang.reflect.Method;

public final class AccessorFieldNameResolver extends AbstractFieldNameResolver {
    public AccessorFieldNameResolver() {
        super(false);
    }

    @Override
    public boolean accept(Method method) {
        return method.isAnnotationPresent(Accessor.class);
    }

    @Override
    public String resolve(Method method) {
        return method.getAnnotation(Accessor.class).value();
    }
}
