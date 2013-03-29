package net.java.ao.schema.info;

import net.java.ao.RawEntity;

import java.lang.reflect.Method;
import java.util.Set;

public interface TableInfo<T extends RawEntity<K>, K>
{

    Class<T> getEntityType();

    String getName();

    FieldInfo<K> getPrimaryKey();

    Set<FieldInfo> getFields();

    FieldInfo getField(Method method);

    FieldInfo getField(String fieldName);

    boolean hasAccessor(Method method);

    boolean hasMutator(Method method);

    boolean hasField(String fieldName);

}
