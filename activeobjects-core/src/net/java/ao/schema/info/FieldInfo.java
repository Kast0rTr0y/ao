package net.java.ao.schema.info;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import net.java.ao.ValueGenerator;
import net.java.ao.types.TypeInfo;

import javax.annotation.Nullable;
import java.lang.reflect.Method;

public interface FieldInfo<T>
{

    String getName();

    String getPolymorphicName();

    boolean isPrimary();

    boolean isNullable();

    boolean isRelational();

    boolean isTransient();

    boolean hasAutoIncrement();

    boolean hasDefaultValue();

    TypeInfo<T> getTypeInfo();

    Class<T> getJavaType();

    boolean hasAccessor();

    Method getAccessor();

    boolean hasMutator();

    Method getMutator();

    Class<? extends ValueGenerator<? extends T>> getGeneratorType();

    Predicate<FieldInfo> IS_REQUIRED = new Predicate<FieldInfo>() {
        @Override
        public boolean apply(FieldInfo fieldInfo) {
            return !(fieldInfo.isNullable() || fieldInfo.hasDefaultValue() || fieldInfo.hasAutoIncrement());
        }
    };

    Predicate<FieldInfo> HAS_GENERATOR = new Predicate<FieldInfo>() {
        @Override
        public boolean apply(FieldInfo fieldInfo) {
            return fieldInfo.getGeneratorType() != null;
        }
    };

    Function<FieldInfo, String> PLUCK_NAME = new Function<FieldInfo, String>() {
        public String apply(FieldInfo fieldInfo) {
            return fieldInfo.getName();
        }
    };

}
