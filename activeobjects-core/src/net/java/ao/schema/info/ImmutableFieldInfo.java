package net.java.ao.schema.info;

import net.java.ao.ValueGenerator;
import net.java.ao.types.TypeInfo;

import java.lang.reflect.Method;

class ImmutableFieldInfo<T> implements FieldInfo {

    private final String fieldName;
    private final String polymorphicName;

    private final Method accessor;
    private final Method mutator;

    private final boolean primary;
    private final boolean nullable;
    private final boolean isTransient;

    private final boolean autoIncrement;
    private final boolean defaultValue;

    private final Class<T> fieldType;
    private final TypeInfo<T> typeInfo;
    private final Class<ValueGenerator<? extends T>> generatorType;

    ImmutableFieldInfo(String fieldName, String polymorphicName, Method accessor, Method mutator,
                       Class<T> fieldType, TypeInfo<T> typeInfo, boolean primary, boolean nullable,
                       boolean isTransient, boolean autoIncrement, boolean defaultValue,
                       Class<ValueGenerator<? extends T>> generatorType) {
        this.fieldName = fieldName;
        this.polymorphicName = polymorphicName;
        this.accessor = accessor;
        this.mutator = mutator;
        this.primary = primary;
        this.nullable = nullable;
        this.isTransient = isTransient;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
        this.fieldType = fieldType;
        this.typeInfo = typeInfo;
        this.generatorType = generatorType;
    }

    @Override
    public String getName() {
        return fieldName;
    }

    @Override
    public String getPolymorphicName() {
        return polymorphicName;
    }

    @Override
    public boolean isPrimary() {
        return primary;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isStorable() {
        return !isTransient() && getTypeInfo().getLogicalType().shouldStore(getJavaType());
    }

    @Override
    public boolean isCacheable() {
        return isStorable();
    }

    @Override
    public boolean isTransient() {
        return isTransient;
    }

    @Override
    public boolean hasAutoIncrement() {
        return autoIncrement;
    }

    @Override
    public boolean hasDefaultValue() {
        return defaultValue;
    }

    @Override
    public TypeInfo<T> getTypeInfo() {
        return typeInfo;
    }

    @Override
    public Class<T> getJavaType() {
        return fieldType;
    }

    @Override
    public boolean hasAccessor() {
        return accessor != null;
    }

    @Override
    public Method getAccessor() {
        return accessor;
    }

    @Override
    public boolean hasMutator() {
        return mutator != null;
    }

    @Override
    public Method getMutator() {
        return mutator;
    }

    @Override
    public Class<? extends ValueGenerator<? extends T>> getGeneratorType() {
        return generatorType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ImmutableFieldInfo)) return false;

        ImmutableFieldInfo that = (ImmutableFieldInfo) o;

        if (accessor != null ? !accessor.equals(that.accessor) : that.accessor != null) return false;
        if (!fieldName.equals(that.fieldName)) return false;
        if (mutator != null ? !mutator.equals(that.mutator) : that.mutator != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fieldName.hashCode();
        result = 31 * result + (accessor != null ? accessor.hashCode() : 0);
        result = 31 * result + (mutator != null ? mutator.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ImmutableFieldInfo{" +
                "fieldName='" + fieldName + '\'' +
                ", polymorphicName='" + polymorphicName + '\'' +
                ", accessor=" + accessor +
                ", mutator=" + mutator +
                ", primary=" + primary +
                ", nullable=" + nullable +
                ", autoIncrement=" + autoIncrement +
                ", defaultValue=" + defaultValue +
                ", fieldType=" + fieldType +
                ", typeInfo=" + typeInfo +
                ", generatorType=" + generatorType +
                '}';
    }
}
