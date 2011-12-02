package net.java.ao.types;

import java.lang.reflect.InvocationTargetException;

import net.java.ao.ActiveObjectsConfigurationException;

public abstract class AbstractNumericType<T> extends DatabaseType<T>
{
    private final NumericTypeProperties properties;
    
    protected AbstractNumericType(int sqlTypeCode, NumericTypeProperties properties, Class<?>... classes)
    {
        super(sqlTypeCode, null, classes);
        this.properties = properties;
    }
    
    @Override
    public String getSqlTypeIdentifier()
    {
        String typeName = properties.getSqlTypeIdentifier();
        if ((properties.getPrecision() != null) && !properties.isIgnorePrecision())
        {
            if ((properties.getScale() != null) && !properties.isIgnoreScale())
            {
                return typeName + "(" + properties.getPrecision() + "," + properties.getScale() + ")";
            }
            else
            {
                return typeName + "(" + properties.getPrecision() + ")";
            }
        }
        return typeName;
    }
    
    @Override
    public AbstractNumericType<T> withPrecision(int precision)
    {
        if (precision <= 0)
        {
            throw new ActiveObjectsConfigurationException("Precision must be > 0");
        }
        return newInstance(properties.withPrecision(precision));
    }
    
    @Override
    public AbstractNumericType<T> withScale(int scale)
    {
        if (scale < 0)
        {
            throw new ActiveObjectsConfigurationException("Scale must be >= 0");
        }
        return newInstance(properties.withScale(scale));
    }
    
    @SuppressWarnings("unchecked")
    protected AbstractNumericType<T> newInstance(NumericTypeProperties properties)
    {
        try
        {
            return this.getClass().getConstructor(NumericTypeProperties.class).newInstance(properties);
        }
        catch (Exception e)
        {
            throw new ActiveObjectsConfigurationException(e);
        }
    }
}
