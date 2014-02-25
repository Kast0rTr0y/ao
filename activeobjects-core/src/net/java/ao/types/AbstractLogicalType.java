package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import net.java.ao.EntityManager;

abstract class AbstractLogicalType<T> implements LogicalType<T>
{
    private final String name;
    private final ImmutableSet<Class<?>> types;
    private final int defaultJdbcWriteType;
    private final ImmutableSet<Integer> jdbcReadTypes;
    
    protected AbstractLogicalType(String name, Class<?>[] types, int defaultJdbcWriteType, Integer[] jdbcReadTypes)
    {
        this.name = name;
        this.types = ImmutableSet.copyOf(types);
        this.defaultJdbcWriteType = defaultJdbcWriteType;
        this.jdbcReadTypes = ImmutableSet.copyOf(jdbcReadTypes);
    }
    
    public String getName()
    {
        return name;
    }
    
    public ImmutableSet<Class<?>> getTypes()
    {
        return types;
    }
    
    public int getDefaultJdbcWriteType()
    {
        return defaultJdbcWriteType;
    }
    
    public ImmutableSet<Integer> getJdbcReadTypes()
    {
        return jdbcReadTypes;
    }
    
    public boolean isAllowedAsPrimaryKey()
    {
        return false;
    }
    
    @SuppressWarnings("unchecked")
    public Object validate(Object value) throws IllegalArgumentException
    {
        if ((value != null) && !isSupportedType(value.getClass()))
        {
            throw new IllegalArgumentException("Value of class " + value.getClass() + " is not valid for column type " + getName());
        }
        return validateInternal((T) value);
    }
    
    private boolean isSupportedType(Class<?> clazz)
    {
        for (Class<?> type : types)
        {
            if (type.isAssignableFrom(clazz))
            {
                return true;
            }
        }
        return false;
    }
    
    protected T validateInternal(T value) throws IllegalArgumentException
    {
        return value;
    }

    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, T value, int jdbcType) throws SQLException
    {
        stmt.setObject(index, value, jdbcType);
    }
    
    public T pullFromDatabase(EntityManager manager, ResultSet res, Class<T> type, int columnIndex) throws SQLException
    {
        return pullFromDatabase(manager, res, type, res.getMetaData().getColumnName(columnIndex));
    }

    public boolean shouldCache(Class<?> type)
    {
        return shouldStore(type);
    }

    public boolean shouldStore(final Class<?> type)
    {
        return true;
    }

    public T parse(String input) throws IllegalArgumentException
    {
        throw new IllegalArgumentException("Cannot parse a string into type " + getName());
    }
    
    public T parseDefault(String input) throws IllegalArgumentException
    {
        return parse(input);
    }
    
    public boolean valueEquals(Object value1, Object value2)
    {
        return Objects.equal(value1, value2);
    }
    
    public String valueToString(T value)
    {
        return String.valueOf(value);
    }
    
    @Override
    public boolean equals(Object other)
    {
        return (getClass() == other.getClass());
    }
    
    @Override
    public String toString()
    {
        return getName();
    }
}