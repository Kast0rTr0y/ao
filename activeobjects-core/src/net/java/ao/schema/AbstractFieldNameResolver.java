package net.java.ao.schema;

import java.lang.reflect.Method;

abstract class AbstractFieldNameResolver implements FieldNameResolver {
    private final boolean transform;

    protected AbstractFieldNameResolver(boolean transform) {
        this.transform = transform;
    }

    public boolean accept(Method method) {
        return false;
    }

    public String resolve(Method method) {
        return null;
    }

    public final boolean transform() {
        return transform;
    }
}
