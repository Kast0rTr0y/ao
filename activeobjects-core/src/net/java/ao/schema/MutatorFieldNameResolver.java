package net.java.ao.schema;

import net.java.ao.Mutator;

import java.lang.reflect.Method;

public final class MutatorFieldNameResolver extends AbstractFieldNameResolver {
    public MutatorFieldNameResolver() {
        super(false);
    }

    @Override
    public boolean accept(Method method) {
        return method.isAnnotationPresent(Mutator.class);
    }

    @Override
    public String resolve(Method method) {
        return method.getAnnotation(Mutator.class).value();
    }
}
