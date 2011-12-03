package net.java.ao.types;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

import net.java.ao.Common;
import net.java.ao.RawEntity;

import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static net.java.ao.types.LogicalTypes.stringType;
import static net.java.ao.types.LogicalTypes.uriType;
import static net.java.ao.types.LogicalTypes.urlType;
import static net.java.ao.types.SchemaProperties.schemaType;
import static net.java.ao.types.TypeQualifiers.UNLIMITED_LENGTH;
import static net.java.ao.types.TypeQualifiers.qualifiers;

/**
 * <p>Central managing class for the ActiveObjects type system.  The type
 * system in AO is designed to allow extensibility and control over
 * how specific data types are handled internally.  All database-agnostic,
 * type-specific tasks are delegated to the actual type instances.  This
 * class acts as a singleton container for every available type, indexing
 * them based on corresponding Java type and JDBC integer type.</p>
 * 
 * <p>This container is thread safe and so may be used from within multiple
 * contexts.</p>
 */
public class TypeManager
{
    private static final ImmutableSet<Integer> UNLIMITED_TEXT_TYPES =
            ImmutableSet.<Integer>of(CLOB, LONGNVARCHAR, LONGVARCHAR);
    
    private final ImmutableMultimap<Class<?>, TypeInfo<?>> classIndex;
    private final ImmutableMultimap<Integer, TypeInfo<?>> jdbcTypeIndex;
    
    private TypeManager(Builder builder)
    {
        this.classIndex = ImmutableMultimap.copyOf(builder.classIndex);
        this.jdbcTypeIndex = ImmutableMultimap.copyOf(builder.jdbcTypeIndex);
    }

    public <T> TypeInfo<T> getType(Class<T> javaType)
    {
        return getType(javaType, qualifiers());
    }

    @SuppressWarnings("unchecked")
    public <T> TypeInfo<T> getType(Class<T> javaType, TypeQualifiers qualifiers)
    {
        if (RawEntity.class.isAssignableFrom(javaType))
        {
            final Class<RawEntity<Object>> entityType = (Class<RawEntity<Object>>) javaType;
            final Class<Object> primaryKeyClass = Common.getPrimaryKeyClassType(entityType);
            final TypeInfo<Object> primaryKeyTypeInfo = getType(primaryKeyClass);
            final LogicalType<RawEntity<Object>> logicalType =
                LogicalTypes.entityType(entityType, primaryKeyTypeInfo, primaryKeyClass);
            return (TypeInfo<T>) new TypeInfo<RawEntity<Object>>(logicalType,
                primaryKeyTypeInfo.getSchemaProperties(),
                primaryKeyTypeInfo.getQualifiers());
        }
        for (Class<?> clazz = javaType; clazz != null; clazz = clazz.getSuperclass())
        {
            if (classIndex.containsKey(clazz))
            {
                return (TypeInfo<T>) findTypeWithQualifiers(classIndex.get(clazz), qualifiers);
            }
        }
        throw new RuntimeException("Unrecognized type: " + javaType.getName());
    }
    
    public TypeInfo<?> getTypeFromSchema(int jdbcType, TypeQualifiers qualifiers)
    {
        if (jdbcTypeIndex.containsKey(jdbcType))
        {
            // If it is an unlimited text type, add unlimited string length to the qualifiers
            if (UNLIMITED_TEXT_TYPES.contains(jdbcType))
            {
                qualifiers = qualifiers.stringLength(TypeQualifiers.UNLIMITED_LENGTH);
            }
            return findTypeWithQualifiers(jdbcTypeIndex.get(jdbcType), qualifiers);
        }
        else
        {
            return null;
        }
    }

    private TypeInfo<?> findTypeWithQualifiers(Iterable<TypeInfo<?>> types, TypeQualifiers qualifiers)
    {
        TypeInfo<?> acceptableType = null;
        for (TypeInfo<?> type : types)
        {
            // The preferred type mapping is one that exactly matches the requested type qualifiers
            // (e.g. the mapping for unlimited-length strings is always used if string length == UNLIMITED).
            // Otherwise we use TypeQualifiers.isCompatibleWith to find the next best match.
            TypeQualifiers typeQualifiers = type.getQualifiers();
            if (typeQualifiers.equals(qualifiers))
            {
                return type;
            }
            else
            {
                if (typeQualifiers.isCompatibleWith(qualifiers))
                {
                    acceptableType = type;
                }
            }
        }
        return acceptableType.withQualifiers(qualifiers);
    }
    
    public static class Builder
    {
        private final SetMultimap<Class<?>, TypeInfo<?>> classIndex = HashMultimap.create();
        private final SetMultimap<Integer, TypeInfo<?>> jdbcTypeIndex = HashMultimap.create();
        
        public TypeManager build()
        {
            return new TypeManager(this);
        }
        
        public <T> Builder addMapping(LogicalType<T> logicalType, SchemaProperties schemaProperties)
        {
            return addMapping(logicalType, schemaProperties, qualifiers());
        }

        public <T> Builder addMapping(LogicalType<T> logicalType, SchemaProperties schemaProperties, TypeQualifiers qualifiers)
        {
            TypeInfo<T> typeInfo = new TypeInfo<T>(logicalType, schemaProperties, qualifiers);
            for (Class<?> clazz : logicalType.getTypes())
            {
                classIndex.put(clazz, typeInfo);
            }
            for (Integer jdbcType : logicalType.getJdbcReadTypes())
            {
                jdbcTypeIndex.put(jdbcType, typeInfo);
            }
            return this;
        }
        
        public Builder addStringTypes(String limitedStringSqlType, String unlimitedStringSqlType)
        {
            addMapping(stringType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                       qualifiers().stringLength(StringType.DEFAULT_LENGTH));
            addMapping(stringType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                       qualifiers().stringLength(UNLIMITED_LENGTH));
            
            addMapping(uriType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                       qualifiers().stringLength(TypeQualifiers.MAX_STRING_LENGTH));
            addMapping(uriType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                       qualifiers().stringLength(UNLIMITED_LENGTH));
            
            addMapping(urlType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                       qualifiers().stringLength(TypeQualifiers.MAX_STRING_LENGTH));
            addMapping(urlType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                       qualifiers().stringLength(UNLIMITED_LENGTH));
            
            return this;
        }
    }
}
