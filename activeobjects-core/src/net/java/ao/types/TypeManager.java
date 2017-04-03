package net.java.ao.types;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import net.java.ao.Common;
import net.java.ao.RawEntity;

import java.sql.Types;

import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.TINYINT;
import static net.java.ao.types.LogicalTypes.blobType;
import static net.java.ao.types.LogicalTypes.booleanType;
import static net.java.ao.types.LogicalTypes.dateType;
import static net.java.ao.types.LogicalTypes.doubleType;
import static net.java.ao.types.LogicalTypes.enumType;
import static net.java.ao.types.LogicalTypes.integerType;
import static net.java.ao.types.LogicalTypes.longType;
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
 * class acts as a container for every available type, indexing
 * them based on corresponding Java type and JDBC integer type.</p>
 *
 * <p>This container is thread safe and so may be used from within multiple
 * contexts.</p>
 */
public class TypeManager {
    private static final ImmutableSet<Integer> UNLIMITED_TEXT_TYPES = ImmutableSet.of(CLOB, LONGNVARCHAR, LONGVARCHAR);

    private final ImmutableMultimap<Class<?>, TypeInfo<?>> classIndex;
    private final ImmutableMultimap<Integer, TypeInfo<?>> jdbcTypeIndex;

    private TypeManager(Builder builder) {
        this.classIndex = ImmutableMultimap.copyOf(builder.classIndex);
        this.jdbcTypeIndex = ImmutableMultimap.copyOf(builder.jdbcTypeIndex);
    }

    public <T> TypeInfo<T> getType(Class<T> javaType) {
        return getType(javaType, qualifiers());
    }

    @SuppressWarnings("unchecked")
    public <T> TypeInfo<T> getType(Class<T> javaType, TypeQualifiers qualifiers) {
        if (RawEntity.class.isAssignableFrom(javaType)) {
            final Class<RawEntity<Object>> entityType = (Class<RawEntity<Object>>) javaType;
            final Class<Object> primaryKeyClass = Common.getPrimaryKeyClassType(entityType);
            final TypeInfo<Object> primaryKeyTypeInfo = getType(primaryKeyClass);
            final LogicalType<RawEntity<Object>> logicalType =
                    LogicalTypes.entityType(entityType, primaryKeyTypeInfo, primaryKeyClass);
            return (TypeInfo<T>) new TypeInfo<RawEntity<Object>>(logicalType,
                    primaryKeyTypeInfo.getSchemaProperties(),
                    primaryKeyTypeInfo.getQualifiers());
        }
        for (Class<?> clazz = javaType; clazz != null; clazz = clazz.getSuperclass()) {
            if (classIndex.containsKey(clazz)) {
                return (TypeInfo<T>) findTypeWithQualifiers(classIndex.get(clazz), qualifiers);
            }
        }
        throw new RuntimeException("Unrecognized type: " + javaType.getName());
    }

    public TypeInfo<?> getTypeFromSchema(int jdbcType, TypeQualifiers qualifiers) {
        if (jdbcTypeIndex.containsKey(jdbcType)) {
            // If it is an unlimited text type, add unlimited string length to the qualifiers
            if (UNLIMITED_TEXT_TYPES.contains(jdbcType)) {
                qualifiers = qualifiers.stringLength(TypeQualifiers.UNLIMITED_LENGTH);
            }
            return findTypeWithQualifiers(jdbcTypeIndex.get(jdbcType), qualifiers);
        } else {
            return null;
        }
    }

    private TypeInfo<?> findTypeWithQualifiers(Iterable<TypeInfo<?>> types, TypeQualifiers qualifiers) {
        TypeInfo<?> acceptableType = null;
        for (TypeInfo<?> type : types) {
            TypeQualifiers typeQualifiers = type.getQualifiers();
            if (typeQualifiers.equals(qualifiers)) {
                // The preferred type mapping is one that exactly matches the requested type qualifiers
                // (e.g. the mapping for unlimited-length strings is always used if string length == UNLIMITED).
                return type;
            } else {
                // Otherwise we should return the next best match, which is any type whose qualifiers can be set
                // in a way that accommodates the requested qualifiers. The constraint we have is whether the type
                // supports unlimited string length or not.
                if (typeQualifiers.isUnlimitedStringLengthSupportCompatible(qualifiers)) {
                    acceptableType = type;
                }
            }
        }
        return acceptableType != null ? acceptableType.withQualifiers(qualifiers) : null;
    }

    public static TypeManager derby() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BLOB"))
                .addMapping(booleanType(), schemaType("SMALLINT").jdbcWriteType(TINYINT).precisionAllowed(true), qualifiers().precision(1))
                .addMapping(dateType(), schemaType("DATETIME"))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "CLOB", Integer.MAX_VALUE)
                .build();
    }

    public static TypeManager hsql() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("LONGVARBINARY"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("DATETIME"))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "LONGVARCHAR", 16 * 1024 * 1024)//LONGVARCHAR defaults to 16M
                .build();
    }

    public static TypeManager h2() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BLOB"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("TIMESTAMP"))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(integerType(), schemaType("INT"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "CLOB", Integer.MAX_VALUE)
                .build();
    }

    public static TypeManager mysql() {
        Builder builder = new Builder()
                .addMapping(blobType(), schemaType("BLOB"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("DATETIME"))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "LONGTEXT", Integer.MAX_VALUE);
        return new MySQLTypeManager(builder);
    }

    public static TypeManager postgres() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BYTEA"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("TIMESTAMP"))
                .addMapping(doubleType(), schemaType("DOUBLE PRECISION"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "TEXT", 1024 * 1024 * 1024)//TEXT defaults to 1G
                .build();
    }

    public static TypeManager sqlServer() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("IMAGE"))
                .addMapping(booleanType(), schemaType("BIT"))
                .addMapping(dateType(), schemaType("DATETIME"))
                .addMapping(doubleType(), schemaType("FLOAT"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("NVARCHAR", "NVARCHAR(max)", (1024 * 1024 * 1024) - 1) // NVARCHAR(max) is (2^30)-1
                .build();
    }

    public static TypeManager oracle() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BLOB"))
                .addMapping(booleanType(), schemaType("NUMBER").precisionAllowed(true), qualifiers().precision(1))
                .addMapping(dateType(), schemaType("TIMESTAMP"))
                .addMapping(doubleType(), schemaType("DOUBLE PRECISION"))
                .addMapping(integerType(), schemaType("NUMBER").precisionAllowed(true), qualifiers().precision(11))
                .addMapping(longType(), schemaType("NUMBER").precisionAllowed(true), qualifiers().precision(20))
                .addStringTypes("VARCHAR", "CLOB", Integer.MAX_VALUE)
                .build();
    }

    public static TypeManager nuodb() {
        return new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BLOB"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("TIMESTAMP").scaleAllowed(true).precisionAllowed(false), qualifiers().scale(6).precision(16))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(integerType(), schemaType("INTEGER").precisionAllowed(false), qualifiers().precision(9))
                .addMapping(longType(), schemaType("BIGINT").precisionAllowed(false))
                .addStringTypes("VARCHAR", "TEXT", Integer.MAX_VALUE)
                .build();
    }

    public static class Builder {
        private final SetMultimap<Class<?>, TypeInfo<?>> classIndex = HashMultimap.create();
        private final SetMultimap<Integer, TypeInfo<?>> jdbcTypeIndex = HashMultimap.create();

        public TypeManager build() {
            return new TypeManager(this);
        }

        public <T> Builder addMapping(LogicalType<T> logicalType, SchemaProperties schemaProperties) {
            return addMapping(logicalType, schemaProperties, qualifiers());
        }

        public <T> Builder addMapping(LogicalType<T> logicalType, SchemaProperties schemaProperties, TypeQualifiers qualifiers) {
            TypeInfo<T> typeInfo = new TypeInfo<T>(logicalType, schemaProperties, qualifiers);
            for (Class<?> clazz : logicalType.getTypes()) {
                classIndex.put(clazz, typeInfo);
            }
            for (Integer jdbcType : logicalType.getJdbcReadTypes()) {
                jdbcTypeIndex.put(jdbcType, typeInfo);
            }
            return this;
        }

        public Builder addStringTypes(String limitedStringSqlType, String unlimitedStringSqlType, final int precision) {
            addMapping(stringType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                    qualifiers().stringLength(StringType.DEFAULT_LENGTH));
            addMapping(stringType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                    qualifiers().stringLength(UNLIMITED_LENGTH).precision(precision));

            addMapping(enumType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                    qualifiers().stringLength(StringType.DEFAULT_LENGTH));

            addMapping(uriType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                    qualifiers().stringLength(TypeQualifiers.MAX_STRING_LENGTH));
            addMapping(uriType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                    qualifiers().stringLength(UNLIMITED_LENGTH).precision(precision));

            addMapping(urlType(), schemaType(limitedStringSqlType).stringLengthAllowed(true),
                    qualifiers().stringLength(TypeQualifiers.MAX_STRING_LENGTH));
            addMapping(urlType(), schemaType(unlimitedStringSqlType).stringLengthAllowed(true).defaultValueAllowed(false),
                    qualifiers().stringLength(UNLIMITED_LENGTH).precision(precision));

            return this;
        }
    }

    private static class MySQLTypeManager extends TypeManager {
        private MySQLTypeManager(final Builder builder) {
            super(builder);
        }

        @Override
        public TypeInfo<?> getTypeFromSchema(final int jdbcType, final TypeQualifiers qualifiers) {
            if (jdbcType == Types.TINYINT && qualifiers.hasPrecision() && qualifiers.getPrecision() == 1) {
                // AO-485: MySQL does not have a "boolean" or "bit" type. It uses tinyint(1) instead. The JDBC driver
                // has internal translation for this, but:
                //   1. It can be turned off using tinyInt1isBit=false on the URL
                //   2. The MariaDB connector doesn't apply it correctly even when it's on
                // This simulates that behaviour. It can't be done using the normal TypeManager internals because
                // the way LogicalType's read types interact with the Builder doesn't allow specifying qualifiers
                // only on specific read types.
                //
                // The MariaDB connector bug is: https://mariadb.atlassian.net/browse/CONJ-72
                return super.getTypeFromSchema(Types.BIT, qualifiers());
            }
            return super.getTypeFromSchema(jdbcType, qualifiers);
        }
    }
}
