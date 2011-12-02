package net.java.ao.types;

public class NumericTypeProperties
{
    private final String sqlTypeIdentifier;
    private final Integer precision;
    private final Integer scale;
    private final boolean ignorePrecision;
    private final boolean ignoreScale;
    
    private NumericTypeProperties(String sqlTypeIdentifier, Integer precision, Integer scale,
                                  boolean ignorePrecision, boolean ignoreScale)
    {
        this.sqlTypeIdentifier = sqlTypeIdentifier;
        this.precision = precision;
        this.scale = scale;
        this.ignorePrecision = ignorePrecision;
        this.ignoreScale = ignoreScale;
    }

    public static NumericTypeProperties numericType(String sqlTypeIdentifier)
    {
        return new NumericTypeProperties(sqlTypeIdentifier, null, null, false, false);
    }

    public NumericTypeProperties withPrecision(int precision)
    {
        return new NumericTypeProperties(this.sqlTypeIdentifier, precision, this.scale, this.ignorePrecision, this.ignoreScale);
    }
    
    public NumericTypeProperties withScale(int scale)
    {
        return new NumericTypeProperties(this.sqlTypeIdentifier, this.precision, scale, this.ignorePrecision, this.ignoreScale);
    }
    
    public NumericTypeProperties ignorePrecision(boolean ignorePrecision)
    {
        return new NumericTypeProperties(this.sqlTypeIdentifier, this.precision, this.scale, ignorePrecision, this.ignoreScale);
    }
    
    public NumericTypeProperties ignoreScale(boolean ignoreScale)
    {
        return new NumericTypeProperties(this.sqlTypeIdentifier, this.precision, this.scale, this.ignorePrecision, ignoreScale);
    }
    
    public String getSqlTypeIdentifier()
    {
        return sqlTypeIdentifier;
    }
    
    public Integer getPrecision()
    {
        return precision;
    }
    
    public Integer getScale()
    {
        return scale;
    }
    
    public boolean isIgnorePrecision()
    {
        return ignorePrecision;
    }
    
    public boolean isIgnoreScale()
    {
        return ignoreScale;
    }
}
