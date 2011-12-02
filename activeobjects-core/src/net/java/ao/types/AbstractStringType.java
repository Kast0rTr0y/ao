package net.java.ao.types;

import java.sql.Types;

import com.google.common.base.Objects;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.schema.StringLength;

abstract class AbstractStringType<T> extends DatabaseType<T>
{
    static final int MAX_PRECISION = 767; // this is MySQL's.
    
    private final StringTypeProperties properties;
    
    protected AbstractStringType(StringTypeProperties properties, Class<?>... classes)
    {
        super(Types.VARCHAR, null, classes);
        this.properties = properties;
    }

    @Override
    public DatabaseType<T> withStringLength(int length)
    {
        if ((length < 0) && (length != StringLength.UNLIMITED))
        {
            throw new ActiveObjectsConfigurationException("@StringLength must be > 0 or UNLIMITED");
        }
        else if (length > MAX_PRECISION)
        {
            throw new ActiveObjectsConfigurationException("@StringLength cannot be > " + MAX_PRECISION);
        }
        return newInstance(properties.withLength(length));
    }
    
    @Override
    public int getType()
    {
        return properties.isUnlimited() ? Types.CLOB : Types.VARCHAR; 
    }
    
    @Override
    public String getSqlTypeIdentifier()
    {
        if (properties.isUnlimited())
        {
            return properties.getSqlTypeIdentifier();
        }
        return properties.getSqlTypeIdentifier() + "(" + properties.getLength() + ")";
    }
    
    @Override
    public T defaultParseValue(String value)
    {
        if (properties.isUnlimited())
        {
            throw new ActiveObjectsConfigurationException("Default value cannot be set for unlimited-length text fields.");
        }
        return parseValueInternal(value);
    }

    protected abstract T parseValueInternal(String value);
    
    @Override
    public boolean valueEquals(Object val1, Object val2)
    {
        return Objects.equal(val1, val2);
    }

    @SuppressWarnings("unchecked")
    protected AbstractStringType<T> newInstance(StringTypeProperties properties)
    {
        try
        {
            return this.getClass().getConstructor(StringTypeProperties.class).newInstance(properties);
        }
        catch (Exception e)
        {
            throw new ActiveObjectsConfigurationException(e);
        }
    }
}
