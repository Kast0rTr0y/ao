package net.java.ao.schema.info;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import net.java.ao.ValueGenerator;
import net.java.ao.types.TypeInfo;

import javax.annotation.Nullable;
import java.lang.reflect.Method;

/**
 * A description of the field generated by the {@link net.java.ao.RawEntity}
 * @param <T>
 * @see FieldInfo
 * @since 0.21
 */
public interface FieldInfo<T>
{

    /**
     * @return the name of the column in the database.
     */
    String getName();

    /**
     * @return the polymorphic name for the field, if one exists
     * @see net.java.ao.Polymorphic
     */
    String getPolymorphicName();

    /**
     * @return {@code true} if the field is the primary key, {@code false} otherwise
     * @see net.java.ao.schema.PrimaryKey
     */
    boolean isPrimary();

    /**
     * @return {@code true} if the field is nullable, {@code false} otherwise
     * @see net.java.ao.schema.NotNull
     */
    boolean isNullable();

    /**
     * @return {@code true} if a value corresponding to this field may be stored by the entity, {@code false} otherwise
     */
    boolean isStorable();

    /**
     * @deprecated since 0.25. Entities and values now no longer cached.
     */
    boolean isCacheable();

    /**
     * @return {@code true} if the field is transient, {@code false} otherwise
     * @see net.java.ao.Transient
     */
    boolean isTransient();

    /**
     * @return {@code true} if the field an auto increment field, {@code false} otherwise
     * @see net.java.ao.schema.AutoIncrement
     * @see net.java.ao.schema.PrimaryKey
     */
    boolean hasAutoIncrement();

    /**
     * @return {@code true} if the field has a default value, {@code false} otherwise
     */
    boolean hasDefaultValue();

    /**
     * @return The database type information for the field
     */
    TypeInfo<T> getTypeInfo();

    /**
     * @return the java type of the field
     */
    Class<T> getJavaType();

    /**
     * @return {@code true} if the field has an accessor, {@code false} otherwise
     */
    boolean hasAccessor();

    /**
     * @return the accessor if one exists
     * @see #hasAccessor()
     */
    Method getAccessor();

    /**
     * @return {@code true} if the field has an mutator, {@code false} otherwise
     */
    boolean hasMutator();

    /**
     * @return the mutator if one exists
     * @see #hasMutator()
     */
    Method getMutator();

    /**
     * @return the class of the value generator if one exists
     * @see net.java.ao.Generator
     */
    Class<? extends ValueGenerator<? extends T>> getGeneratorType();

    /**
     * A predicate for filtering required fields
     * @see #isNullable()
     * @see #hasDefaultValue()
     * @see #hasAutoIncrement()
     */
    Predicate<FieldInfo> IS_REQUIRED = new Predicate<FieldInfo>() {
        @Override
        public boolean apply(FieldInfo fieldInfo) {
            return !(fieldInfo.isNullable() || fieldInfo.hasDefaultValue() || fieldInfo.hasAutoIncrement());
        }
    };

    /**
     * A predicate for filtering fields with value generators
     * @see #getGeneratorType()
     */
    Predicate<FieldInfo> HAS_GENERATOR = new Predicate<FieldInfo>() {
        @Override
        public boolean apply(FieldInfo fieldInfo) {
            return fieldInfo.getGeneratorType() != null;
        }
    };

    /**
     * Return the name of the field
     */
    Function<FieldInfo, String> PLUCK_NAME = new Function<FieldInfo, String>() {
        public String apply(FieldInfo fieldInfo) {
            return fieldInfo.getName();
        }
    };

}
