package net.java.ao.types;

import net.java.ao.ActiveObjectsConfigurationException;

import static net.java.ao.types.TypeQualifiers.qualifiers;

/**
 * Describes a mapping between a {@link LogicalType}, with optional qualifiers, and the
 * underlying database type.
 * <p>
 * The database provider constructs one instance of this class for each supported logical type,
 * and may provide additional derived instances for specific entity properties.  The caller
 * will never construct an instance of this class directly.
 */
public final class TypeInfo<T>
{
    private final LogicalType<T> logicalType;
    private final SchemaProperties schemaProperties;
    private final TypeQualifiers defaultQualifiers;
    private final TypeQualifiers qualifiers;

    public TypeInfo(LogicalType<T> logicalType,
                    SchemaProperties schemaProperties,
                    TypeQualifiers defaultQualifiers)
    {
        this(logicalType, schemaProperties, defaultQualifiers, qualifiers());
    }

    protected TypeInfo(LogicalType<T> logicalType,
                       SchemaProperties schemaProperties,
                       TypeQualifiers defaultQualifiers,
                       TypeQualifiers qualifiers)
    {
        this.logicalType = logicalType;
        this.schemaProperties = schemaProperties;
        this.defaultQualifiers = defaultQualifiers;
        this.qualifiers = qualifiers;
    }

    /**
     * Returns the {@link LogicalType} describing what Java types are supported by this
     * type and how they are mapped to JDBC operations.
     */
    public LogicalType<T> getLogicalType()
    {
        return logicalType;
    }
    
    /**
     * Returns the {@link SchemaProperties} describing the SQL representation of this type,
     * as specified by the database provider.
     */
    public SchemaProperties getSchemaProperties()
    {
        return schemaProperties;
    }
    
    /**
     * Returns the {@link TypeQualifiers} describing optional length/precision modifiers for this
     * type, which may include defaults set by the database provider and/or values specified by
     * annotations for particular entity properties.
     */
    public TypeQualifiers getQualifiers()
    {
        return defaultQualifiers.withQualifiers(qualifiers);
    }

    /**
     * Returns a boolean specifying if this type can be used as a primary key value.
     * @return if it's allowed or not
     */
    public boolean isAllowedAsPrimaryKey()
    {
        return logicalType.isAllowedAsPrimaryKey() && (!qualifiers.isUnlimitedLength());
    }

    /**
     * Returns the SQL type identifier for this type, including any length or precision modifiers.
     */
    public String getSqlTypeIdentifier()
    {
        TypeQualifiers allQualifiers = getQualifiers();
        if (!allQualifiers.isDefined())
        {
            return schemaProperties.getSqlTypeName();
        }
        StringBuffer ret = new StringBuffer();
        ret.append(schemaProperties.getSqlTypeName());
        if (schemaProperties.isPrecisionAllowed() && allQualifiers.hasPrecision())
        {
            ret.append("(").append(allQualifiers.getPrecision());
            if (schemaProperties.isScaleAllowed() && allQualifiers.hasScale())
            {
                ret.append(",").append(allQualifiers.getScale());
            }
            ret.append(")");
        }
        else if (schemaProperties.isStringLengthAllowed() && allQualifiers.hasStringLength() && !allQualifiers.isUnlimitedLength())
        {
            ret.append("(").append(allQualifiers.getStringLength()).append(")");
        }
        return ret.toString();
    }
    
    /**
     * Returns the JDBC type code to use when passing values to the database.  This is based on the
     * default value from the {@link LogicalType} but can be overridden by each database provider
     * in the {@link SchemaProperties}.
     */
    public int getJdbcWriteType()
    {
        return schemaProperties.hasOverrideJdbcWriteType() ? schemaProperties.getOverrideJdbcWriteType().intValue() :
            logicalType.getDefaultJdbcWriteType();
    }
    
    /**
     * Returns a new instance of this class with the same properties, but with the default
     * {@link TypeQualifiers} overridden by the specified values.
     * @throws ActiveObjectsConfigurationException  if the qualifiers are not allowed for this type
     */
    public TypeInfo<T> withQualifiers(TypeQualifiers qualifiers)
    {
        if (qualifiers.hasStringLength() && !schemaProperties.isStringLengthAllowed())
        {
            throw new ActiveObjectsConfigurationException("String length cannot be specified for the type "
                + logicalType.getName());
        }
        return new TypeInfo<T>(this.logicalType, this.schemaProperties, defaultQualifiers,
                               this.qualifiers.withQualifiers(qualifiers));
    }
    
    /**
     * Returns true if this type is compatible with the given qualifiers.
     */
    public boolean acceptsQualifiers(TypeQualifiers qualifiers)
    {
        return (!qualifiers.hasPrecision() || schemaProperties.isPrecisionAllowed())
            && (!qualifiers.hasScale() || schemaProperties.isScaleAllowed())
            && (!qualifiers.hasStringLength() || schemaProperties.isStringLengthAllowed());
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (other instanceof TypeInfo<?>)
        {
            TypeInfo<?> type = (TypeInfo<?>) other;
            return type.logicalType.equals(logicalType)
                && type.schemaProperties.equals(schemaProperties)
                && type.defaultQualifiers.equals(defaultQualifiers)
                && type.qualifiers.equals(qualifiers);
        }
        
        return false;
    }
    
    @Override
    public int hashCode()
    {
        return ((((logicalType.hashCode() * 31) + schemaProperties.hashCode()) * 31)
            + defaultQualifiers.hashCode() * 31) + qualifiers.hashCode();
    }
    
    /**
     * Describes the type mapping in a format that includes the logical type name, any
     * non-default qualifiers, and the SQL type name, e.g. "String(255):VARCHAR".
     */
    @Override
    public String toString()
    {
        StringBuffer ret = new StringBuffer();
        ret.append(logicalType.getName());
        if (qualifiers.isDefined())
        {
            ret.append("(");
            if (qualifiers.hasPrecision())
            {
                ret.append(qualifiers.getPrecision());
                if (qualifiers.hasScale())
                {
                    ret.append(", ").append(qualifiers.getScale());
                }
            }
            else if (qualifiers.hasStringLength())
            {
                ret.append(qualifiers.getStringLength());
            }
            ret.append(")");
        }
        ret.append(":");
        ret.append(schemaProperties.getSqlTypeName());
        return ret.toString();
    }
}
