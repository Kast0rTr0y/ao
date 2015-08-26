package net.java.ao.schema;

import java.lang.reflect.Method;

public final class IgnoredFieldNameResolver extends AbstractFieldNameResolver {
    public IgnoredFieldNameResolver() {
        super(false);
    }

    @Override
    public boolean accept(Method method) {
        return method.isAnnotationPresent(Ignore.class);
    }
}
