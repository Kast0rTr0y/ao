package net.java.ao.schema.info;

import net.java.ao.RawEntity;

import java.lang.reflect.Method;

public interface SchemaInfo<T extends RawEntity<?>>
{

    Class<T> getEntityType();

    String getFieldName(Method method);

    Class<?> getFieldType(String fieldName);

    String getPolymorphicName(String fieldName);

    boolean hasAccessor(Method method);

    boolean hasField(String fieldName);

    boolean hasMutator(Method method);

    boolean hasPolymorphic(String fieldName);

}
