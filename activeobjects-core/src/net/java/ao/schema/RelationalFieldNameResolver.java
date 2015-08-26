package net.java.ao.schema;

import net.java.ao.Common;

import java.lang.reflect.Method;

public final class RelationalFieldNameResolver extends AbstractFieldNameResolver {
    public RelationalFieldNameResolver() {
        super(false);
    }

    @Override
    public boolean accept(Method method) {
        return Common.isAnnotatedAsRelational(method);
    }
}
