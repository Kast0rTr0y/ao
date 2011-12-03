package net.java.ao.types;

import com.google.common.base.Objects;

import net.java.ao.ActiveObjectsConfigurationException;

/**
 * Describes optional modifiers to a type:  string length for string types, precision and scale
 * for numeric types.  Each {@link TypeInfo} may have default qualifiers set by the database
 * provider; qualifiers may also be modified by annotations.
 */
public class TypeQualifiers
{
    /**
     * Maximum string length (for strings of limited length) cannot be set to greater than this
     * value.  This is the lowest common denominator for maximum string length in all supported
     * databases.
     */
    public static final int MAX_STRING_LENGTH = 767;
    
    /**
     * If {@code stringLength} is set to this constant, the field is an unlimited-length string
     * (TEXT, CLOB, or LONG VARCHAR).
     */
    public static final int UNLIMITED_LENGTH = -1;
    
    private final Integer precision;
    private final Integer scale;
    private final Integer stringLength;
    private final Class<?> propertyType;
    
    private TypeQualifiers(Integer precision, Integer scale, Integer stringLength, Class<?> propertyType)
    {
        this.precision = precision;
        this.scale = scale;
        this.stringLength = stringLength;
        this.propertyType = propertyType;
    }

    public static TypeQualifiers qualifiers()
    {
        return new TypeQualifiers(null, null, null, null);
    }

    public TypeQualifiers precision(int precision)
    {
        if (precision <= 0)
        {
            throw new ActiveObjectsConfigurationException("Numeric precision must be greater than zero");
        }
        return new TypeQualifiers(precision, this.scale, this.stringLength, this.propertyType);
    }
    
    public TypeQualifiers scale(int scale)
    {
        if (scale < 0)
        {
            throw new ActiveObjectsConfigurationException("Numeric scale must be greater than or equal to zero");
        }
        return new TypeQualifiers(this.precision, scale, this.stringLength, this.propertyType);
    }
    
    public TypeQualifiers stringLength(int stringLength)
    {
        if (stringLength != UNLIMITED_LENGTH)
        {
            if (stringLength <= 0)
            {
                throw new ActiveObjectsConfigurationException("String length must be greater than zero or unlimited");
            }
            else if (stringLength > MAX_STRING_LENGTH)
            {
                stringLength = UNLIMITED_LENGTH;
            }
        }
        return new TypeQualifiers(this.precision, this.scale, stringLength, this.propertyType);
    }
    
    public TypeQualifiers propertyType(Class<?> propertyType)
    {
        return new TypeQualifiers(this.precision, this.scale, this.stringLength, propertyType);
    }
    
    public TypeQualifiers withQualifiers(TypeQualifiers overrides)
    {
        if (overrides.isDefined())
        {
            return new TypeQualifiers(overrides.hasPrecision() ? overrides.precision : this.precision,
                                      overrides.hasScale() ? overrides.scale : this.scale,
                                      overrides.hasStringLength() ? overrides.stringLength : this.stringLength,
                                      overrides.hasPropertyType() ? overrides.propertyType : this.propertyType);
        }
        return this;
    }
    
    public Integer getPrecision()
    {
        return precision;
    }
    
    public Integer getScale()
    {
        return scale;
    }
    
    public Integer getStringLength()
    {
        return stringLength;
    }
    
    public Class<?> getPropertyType()
    {
        return propertyType;
    }
    
    public boolean isDefined()
    {
        return hasPrecision() || hasScale() || hasStringLength() || hasPropertyType();
    }
    
    public boolean hasPrecision()
    {
        return (precision != null);
    }
    
    public boolean hasScale()
    {
        return (scale != null);
    }
    
    public boolean hasStringLength()
    {
        return (stringLength != null);
    }
    
    public boolean isUnlimitedLength()
    {
        return ((stringLength != null) && (stringLength == UNLIMITED_LENGTH));
    }
    
    public boolean hasPropertyType()
    {
        return (propertyType != null);
    }
    
    public boolean isCompatibleWith(TypeQualifiers other)
    {
        if (hasStringLength())
        {
            return (isUnlimitedLength() == other.isUnlimitedLength());
        }
        return true;
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (other instanceof TypeQualifiers)
        {
            TypeQualifiers q = (TypeQualifiers) other;
            return Objects.equal(precision, q.precision)
                && Objects.equal(scale, q.scale)
                && Objects.equal(stringLength, q.stringLength);
        }
        return false;
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hashCode(precision, scale, stringLength);
    }
    
    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder("(");
        if (precision != null)
        {
            ret.append("precision=").append(precision);
        }
        if (scale != null)
        {
            if (ret.length() > 1)
            {
                ret.append(",");
            }
            ret.append("scale=").append(scale);
        }
        if (stringLength != null)
        {
            ret.append("length=").append(stringLength);
        }
        return ret.append(")").toString();
    }
}
