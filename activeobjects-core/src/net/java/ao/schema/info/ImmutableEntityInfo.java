package net.java.ao.schema.info;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.java.ao.RawEntity;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

class ImmutableEntityInfo<T extends RawEntity<K>, K> implements EntityInfo<T, K> {

    private final Class<T> entityType;
    private final String tableName;
    private final FieldInfo<K> primaryKey;
    private final Map<String, FieldInfo> fieldByName;
    private final Map<Method, FieldInfo> fieldByMethod;

    ImmutableEntityInfo(
            Class<T> entityType,
            String tableName,
            Set<FieldInfo> fields) {
        this.entityType = checkNotNull(entityType, "entityType");
        this.tableName = checkNotNull(tableName, "tableName");

        ImmutableMap.Builder<String, FieldInfo> fieldByNameBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Method, FieldInfo> fieldByMethodBuilder = ImmutableMap.builder();
        FieldInfo primaryKey = null;
        for (FieldInfo field : fields) {
            fieldByNameBuilder.put(field.getName(), field);
            if (field.getPolymorphicName() != null) {
                fieldByNameBuilder.put(field.getPolymorphicName(), field);
            }
            if (field.isPrimary()) {
                primaryKey = field;
            }
            if (field.hasAccessor()) {
                fieldByMethodBuilder.put(field.getAccessor(), field);
            }
            if (field.hasMutator()) {
                fieldByMethodBuilder.put(field.getMutator(), field);
            }
        }
        fieldByName = fieldByNameBuilder.build();
        fieldByMethod = fieldByMethodBuilder.build();
        //noinspection unchecked
        this.primaryKey = checkNotNull(primaryKey, "primaryKey");
    }

    @Override
    public Class<T> getEntityType() {
        return entityType;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public FieldInfo<K> getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public Set<FieldInfo> getFields() {
        return ImmutableSet.copyOf(fieldByName.values());
    }

    @Override
    public Set<String> getFieldNames() {
        return ImmutableSet.copyOf(Collections2.transform(getFields(), FieldInfo.PLUCK_NAME));
    }

    @Override
    public FieldInfo getField(Method method) {
        return fieldByMethod.get(method);
    }

    @Override
    public FieldInfo getField(String fieldName) {
        return fieldByName.get(fieldName);
    }

    @Override
    public boolean hasAccessor(Method method) {
        FieldInfo field = fieldByMethod.get(method);
        return field != null && method.equals(field.getAccessor());
    }

    @Override
    public boolean hasMutator(Method method) {
        FieldInfo field = fieldByMethod.get(method);
        return field != null && method.equals(field.getMutator());
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldByName.containsKey(fieldName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImmutableEntityInfo that = (ImmutableEntityInfo) o;

        return !(entityType != null ? !entityType.equals(that.entityType) : that.entityType != null);

    }

    @Override
    public int hashCode() {
        return entityType != null ? entityType.hashCode() : 0;
    }
}
