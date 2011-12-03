package net.java.ao.types;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Describes the underlying SQL schema type that corresponds to a given {@link TypeInfo}.
 * This is determined by the database provider.
 */
public class SchemaProperties
{
    private final String sqlTypeName;
    private final Integer overrideJdbcWriteType;
    private final boolean precisionAllowed;
    private final boolean scaleAllowed;
    private final boolean stringLengthAllowed;
    private final boolean defaultValueAllowed;
    
    private SchemaProperties(String sqlTypeName,
                             Integer overrideJdbcWriteType,
                             boolean precisionAllowed,
                             boolean scaleAllowed,
                             boolean stringLengthAllowed,
                             boolean defaultValueAllowed)
    {
        this.sqlTypeName = sqlTypeName;
        this.overrideJdbcWriteType = overrideJdbcWriteType;
        this.precisionAllowed = precisionAllowed;
        this.scaleAllowed = scaleAllowed;
        this.stringLengthAllowed = stringLengthAllowed;
        this.defaultValueAllowed = defaultValueAllowed;
    }
    
    /**
     * Constructs an instance that refers to the specified SQL type name (e.g. "VARCHAR").
     */
    public static SchemaProperties schemaType(String sqlTypeName)
    {
        return new SchemaProperties(checkNotNull(sqlTypeName),
                                    null, false, false, false, true);
    }
    
    /**
     * Returns a new instance with the same properties, but specifying a JDBC type code.
     * This overrides the value given by the {@link LogicalType}.
     */
    public SchemaProperties jdbcWriteType(int jdbcWriteType)
    {
        return new SchemaProperties(this.sqlTypeName,
                                    jdbcWriteType,
                                    this.precisionAllowed,
                                    this.scaleAllowed,
                                    this.stringLengthAllowed,
                                    this.defaultValueAllowed);
    }
    
    /**
     * Returns a new instance with the same properties, but specifying whether or not the
     * SQL type can include a numeric precision qualifier.  This is false by default.
     */
    public SchemaProperties precisionAllowed(boolean precisionAllowed)
    {
        return new SchemaProperties(this.sqlTypeName,
                                    this.overrideJdbcWriteType,
                                    precisionAllowed,
                                    this.scaleAllowed,
                                    this.stringLengthAllowed,
                                    this.defaultValueAllowed);
    }

    /**
     * Returns a new instance with the same properties, but specifying whether or not the
     * SQL type can include a numeric scale qualifier.  This is false by default.
     */
    public SchemaProperties scaleAllowed(boolean scaleAllowed)
    {
        return new SchemaProperties(this.sqlTypeName,
                                    this.overrideJdbcWriteType,
                                    this.precisionAllowed,
                                    scaleAllowed,
                                    this.stringLengthAllowed,
                                    this.defaultValueAllowed);
    }

    /**
     * Returns a new instance with the same properties, but specifying whether or not the
     * SQL type can include a string length qualifier.  This is false by default.
     */
    public SchemaProperties stringLengthAllowed(boolean stringLengthAllowed)
    {
        return new SchemaProperties(this.sqlTypeName,
                                    this.overrideJdbcWriteType,
                                    this.precisionAllowed,
                                    this.scaleAllowed,
                                    stringLengthAllowed,
                                    this.defaultValueAllowed);
    }

    /**
     * Returns a new instance with the same properties, but specifying whether or not a
     * column of this type can have a default value.  This is true by default.
     */
    public SchemaProperties defaultValueAllowed(boolean defaultValueAllowed)
    {
        return new SchemaProperties(this.sqlTypeName,
                                    this.overrideJdbcWriteType,
                                    this.precisionAllowed,
                                    this.scaleAllowed,
                                    this.stringLengthAllowed,
                                    defaultValueAllowed);
    }

    /**
     * Returns the SQL type name (without any precision or length qualifiers).
     */
    public String getSqlTypeName()
    {
        return sqlTypeName;
    }
    
    /**
     * Returns the JDBC type code to be used when writing to the database (e.g.
     * {@link java.sql.Types#VARCHAR}), if this is different than the default from the
     * {@link LogicalType}.
     */
    public Integer getOverrideJdbcWriteType()
    {
        return overrideJdbcWriteType;
    }
    
    /**
     * Returns true if this instance specifies a JDBC type code that is different than
     * the default from the {@link LogicalType}.
     */
    public boolean hasOverrideJdbcWriteType()
    {
        return (overrideJdbcWriteType != null);
    }
    
    /**
     * Returns true if this type is allowed to have a numeric precision qualifier.
     */
    public boolean isPrecisionAllowed()
    {
        return precisionAllowed;
    }
    
    /**
     * Returns true if this type is allowed to have a numeric scale qualifier.
     */
    public boolean isScaleAllowed()
    {
        return scaleAllowed;
    }
    
    /**
     * Returns true if this type is allowed to have a string length qualifier.
     */
    public boolean isStringLengthAllowed()
    {
        return stringLengthAllowed;
    }
    
    /**
     * Returns true if a column of this type is allowed to have a default value.
     */
    public boolean isDefaultValueAllowed()
    {
        return defaultValueAllowed;
    }
}
