package net.java.ao.schema;

import java.lang.reflect.Method;

public final class NullFieldNameResolver extends AbstractFieldNameResolver {
    public NullFieldNameResolver() {
        super(false);
    }

    public boolean accept(Method method) {
        return true;
    }
}
