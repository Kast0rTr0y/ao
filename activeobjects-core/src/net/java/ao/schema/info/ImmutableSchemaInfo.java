package net.java.ao.schema.info;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.java.ao.RawEntity;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class ImmutableSchemaInfo<T extends RawEntity<?>> implements SchemaInfo<T>
{

    private final Class<T> entityType;
    private final ImmutableSet<Method> accessors;
    private final ImmutableSet<Method> mutators;
    private final Map<Method, String> fieldNameByMethod;
    private final Map<String, String> polyNameByFieldName;
    private final Map<String, Class<?>> fieldTypeByFieldName;

    protected ImmutableSchemaInfo(
            Class<T> entityType,
            Set<Method> accessors,
            Set<Method> mutators,
            Map<Method, String> fieldNameByMethod,
            Map<String, String> polyNameByFieldName,
            Map<String, Class<?>> fieldTypeByFieldName)
    {
        this.entityType = entityType;
        this.accessors = ImmutableSet.copyOf(accessors);
        this.mutators = ImmutableSet.copyOf(mutators);
        this.fieldNameByMethod = ImmutableMap.copyOf(fieldNameByMethod);
        this.polyNameByFieldName = ImmutableMap.copyOf(polyNameByFieldName);
        this.fieldTypeByFieldName = ImmutableMap.copyOf(fieldTypeByFieldName);
    }

    @Override
    public Class<T> getEntityType()
    {
        return entityType;
    }

    @Override
    public boolean hasField(String fieldName)
    {
        return fieldTypeByFieldName.containsKey(fieldName);
    }

    @Override
    public boolean hasAccessor(Method method)
    {
        return accessors.contains(method);
    }

    @Override
    public boolean hasMutator(Method method)
    {
        return mutators.contains(method);
    }

    @Override
    public boolean hasPolymorphic(String fieldName)
    {
        return polyNameByFieldName.containsKey(fieldName);
    }

    @Override
    public String getFieldName(Method method)
    {
        return fieldNameByMethod.get(method);
    }

    @Override
    public Class<?> getFieldType(String fieldName)
    {
        return fieldTypeByFieldName.get(fieldName);
    }

    @Override
    public String getPolymorphicName(String fieldName)
    {
        return polyNameByFieldName.get(fieldName);
    }

}
