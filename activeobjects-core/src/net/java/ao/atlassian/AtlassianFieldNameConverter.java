package net.java.ao.atlassian;

import net.java.ao.schema.AbstractFieldNameConverter;
import net.java.ao.schema.AccessorFieldNameResolver;
import net.java.ao.schema.Case;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.FieldNameProcessor;
import net.java.ao.schema.FieldNameResolver;
import net.java.ao.schema.GetterFieldNameResolver;
import net.java.ao.schema.IgnoredFieldNameResolver;
import net.java.ao.schema.IsAFieldNameResolver;
import net.java.ao.schema.MutatorFieldNameResolver;
import net.java.ao.schema.NullFieldNameResolver;
import net.java.ao.schema.PrimaryKeyFieldNameResolver;
import net.java.ao.schema.RelationalFieldNameResolver;
import net.java.ao.schema.SetterFieldNameResolver;
import net.java.ao.schema.UnderscoreFieldNameConverter;

import java.lang.reflect.Method;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static net.java.ao.atlassian.ConverterUtils.MAX_LENGTH;
import static net.java.ao.atlassian.ConverterUtils.checkLength;

public final class AtlassianFieldNameConverter implements FieldNameConverter, FieldNameProcessor {
    private AbstractFieldNameConverter fieldNameConverter;

    public AtlassianFieldNameConverter() {
        fieldNameConverter = new UnderscoreFieldNameConverter(Case.UPPER, newArrayList(
                new IgnoredFieldNameResolver(),
                new RelationalFieldNameResolver(),
                new TransformingFieldNameResolver(new MutatorFieldNameResolver()),
                new TransformingFieldNameResolver(new AccessorFieldNameResolver()),
                new TransformingFieldNameResolver(new PrimaryKeyFieldNameResolver()),
                new GetterFieldNameResolver(),
                new SetterFieldNameResolver(),
                new IsAFieldNameResolver(),
                new NullFieldNameResolver()
        ));
    }

    @Override
    public String getName(Method method) {
        final String name = fieldNameConverter.getName(method);
        return checkLength(name,
                "Invalid entity, generated field name (" + name + ") for method '" + method + "' is too long! " +
                        "It should be no longer than " + MAX_LENGTH + " chars.");
    }

    @Override
    public String getPolyTypeName(Method method) {
        final String name = fieldNameConverter.getPolyTypeName(method);
        return checkLength(name,
                "Invalid entity, generated field polymorphic type name (" + name + ") for method '" + method + "' is too long! " +
                        "It should be no longer than " + MAX_LENGTH + " chars.");
    }

    @Override
    public String convertName(String name) {
        return fieldNameConverter.convertName(name);
    }

    private static final class TransformingFieldNameResolver implements FieldNameResolver {
        private final FieldNameResolver delegate;

        public TransformingFieldNameResolver(FieldNameResolver delegate) {
            this.delegate = checkNotNull(delegate);
        }

        @Override
        public boolean accept(Method method) {
            return delegate.accept(method);
        }

        @Override
        public String resolve(Method method) {
            return delegate.resolve(method);
        }

        @Override
        public boolean transform() {
            return true;
        }
    }
}
