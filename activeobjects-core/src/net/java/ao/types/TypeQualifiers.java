package net.java.ao.types;

import com.google.common.base.Objects;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.schema.StringLength;

/**
 * Describes optional modifiers to a type:  string length for string types, precision and scale
 * for numeric types.  Each {@link TypeInfo} may have default qualifiers set by the database
 * provider; qualifiers may also be modified by annotations.
 */
public class TypeQualifiers
{
    /**
     * String sizes were tuned down due to a SQL server limitation, this has the undesired side effect that
     * existing tables read with lengths between 450 and 767 are incorrectly seen as clobs
     */
    public static final int OLD_MAX_STRING_LENGTH = 767;

    /**
     * Maximum string length (for strings of limited length) cannot be set to greater than this
     * value.  This is the lowest common denominator for maximum string length in all supported
     * databases.
     */
    public static final int MAX_STRING_LENGTH = OLD_MAX_STRING_LENGTH;

    /**
     * If {@code stringLength} is set to this constant, the field is an unlimited-length string
     * (TEXT, CLOB, or LONG VARCHAR).
     */
    public static final int UNLIMITED_LENGTH = StringLength.UNLIMITED;
    
    private final Integer precision;
    private final Integer scale;
    private final Integer stringLength;
    private final Integer reportedStringLength;

    private TypeQualifiers(Integer precision, Integer scale, Integer stringLength, Integer reportedStringLength)
    {
        this.precision = precision;
        this.scale = scale;
        this.stringLength = stringLength;
        this.reportedStringLength = reportedStringLength;
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
        return new TypeQualifiers(precision, this.scale, this.stringLength, this.reportedStringLength);
    }
    
    public TypeQualifiers scale(int scale)
    {
        if (scale < 0)
        {
            throw new ActiveObjectsConfigurationException("Numeric scale must be greater than or equal to zero");
        }
        return new TypeQualifiers(this.precision, scale, this.stringLength, this.reportedStringLength);
    }
    
    public TypeQualifiers stringLength(int stringLength)
    {
        int reportedStringLength = stringLength;
        if (stringLength != UNLIMITED_LENGTH)
        {
            if (stringLength <= 0)
            {
                throw new ActiveObjectsConfigurationException("String length must be greater than zero or unlimited");
            }
            else if (stringLength > MAX_STRING_LENGTH)
            {
                // There is a separate check in SchemaGenerator.getSQLTypeFromMethod to raise an
                // error if someone explicitly tries to specify a length greater than this limit with
                // the @StringLength annotation.  But we can't put that check here, because we also
                // use TypeQualifiers for metadata that is read directly from a database schema, and
                // some databases like to report very large numbers for the length of what is really
                // an unlimited-length (CLOB) column.
                stringLength = UNLIMITED_LENGTH;
                if (reportedStringLength > OLD_MAX_STRING_LENGTH)
                {
                    reportedStringLength = UNLIMITED_LENGTH;
                }
            }
        }
        return new TypeQualifiers(this.precision, this.scale, stringLength, reportedStringLength);
    }

    public TypeQualifiers withQualifiers(TypeQualifiers overrides)
    {
        if (overrides.isDefined())
        {
            return new TypeQualifiers(overrides.hasPrecision() ? overrides.precision : this.precision,
                                      overrides.hasScale() ? overrides.scale : this.scale,
                                      overrides.hasStringLength() ? overrides.stringLength : this.stringLength,
                                      overrides.hasStringLength() ? overrides.reportedStringLength : this.reportedStringLength);
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

    public boolean isDefined()
    {
        return hasPrecision() || hasScale() || hasStringLength();
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
    /**
     *  If there is a disparity between reportedLength and the stringLength then the field is incompatible
     *
     */
    public boolean areLengthsCorrect()
    {
        return Objects.equal(stringLength, reportedStringLength);
    }

    public boolean isUnlimitedStringLengthSupportCompatible(TypeQualifiers other)
    {
        if (hasStringLength() || other.hasStringLength())
        {
            return (isUnlimitedLength() == other.isUnlimitedLength());
        }
        return true;
    }

    /*
     * Even when the schema hasn't changed, we can get into a mismatch when comparing qualifiers that come from a requested logical type
     * versus qualifiers that come from an existing column in the database. Therefore this method determines if the mismatch is due to the
     * ambiguity when going from logical type -> physical type.
     * versus qualifiers that come from an existing column in the database. This method determines if the mismatch is due to the
     * ambiguity when going from logical type -> physical type, and should be used to compare qualifiers derived from entity annotations
     * versus those derived from table metadata.
     */
    public static boolean areCompatible(TypeQualifiers derivedFromEntityAnnotations, TypeQualifiers derivedFromTableMetadata)
    {
        if (derivedFromEntityAnnotations.hasPrecision() && !Objects.equal(derivedFromEntityAnnotations.getPrecision(), derivedFromTableMetadata.getPrecision()))
        {
            return false;
        }
        else if (derivedFromEntityAnnotations.hasScale() && !Objects.equal(derivedFromEntityAnnotations.getScale(), derivedFromTableMetadata.getScale()))
        {
            return false;
        }
        return derivedFromEntityAnnotations.isUnlimitedStringLengthSupportCompatible(derivedFromTableMetadata) &&
                (derivedFromEntityAnnotations.areLengthsCorrect() && derivedFromTableMetadata.areLengthsCorrect());
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (other instanceof TypeQualifiers)
        {
            TypeQualifiers q = (TypeQualifiers) other;
            return Objects.equal(precision, q.precision)
                && Objects.equal(scale, q.scale)
                && Objects.equal(stringLength, q.stringLength)
                && Objects.equal(reportedStringLength, q.reportedStringLength);
        }
        return false;
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hashCode(precision, scale, stringLength, reportedStringLength);
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
