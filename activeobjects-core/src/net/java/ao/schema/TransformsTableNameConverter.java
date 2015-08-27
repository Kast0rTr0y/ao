package net.java.ao.schema;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.convertSimpleClassName;

abstract class TransformsTableNameConverter extends CanonicalClassNameTableNameConverter {
    private List<Transform> transforms;
    private CanonicalClassNameTableNameConverter delegateTableNameConverter;

    TransformsTableNameConverter(List<Transform> transforms, CanonicalClassNameTableNameConverter delegateTableNameConverter) {
        this.transforms = checkNotNull(transforms);
        this.delegateTableNameConverter = checkNotNull(delegateTableNameConverter);
    }

    @Override
    protected final String getName(String entityClassCanonicalName) {
        return delegateTableNameConverter.getName(transform(entityClassCanonicalName));
    }

    private String transform(String entityClassCanonicalName) {
        for (Transform transform : transforms) {
            if (transform.accept(entityClassCanonicalName)) {
                return transform.apply(entityClassCanonicalName);
            }
        }
        return entityClassCanonicalName;
    }

    static interface Transform {
        boolean accept(String entityClassCanonicalName);

        String apply(String entityClassCanonicalName);
    }

    final static class ClassNameTableNameConverter extends CanonicalClassNameTableNameConverter {
        @Override
        protected String getName(String entityClassCanonicalName) {
            return convertSimpleClassName(entityClassCanonicalName);
        }
    }
}
