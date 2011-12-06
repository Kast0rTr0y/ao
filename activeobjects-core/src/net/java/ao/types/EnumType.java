package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static java.sql.Types.VARCHAR;

final class EnumType extends AbstractLogicalType<Enum<?>>
{
    public EnumType()
    {
        super("Enum",
              new Class<?>[] { Enum.class },
              VARCHAR, new Integer[] { });
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Enum<?> pullFromDatabase(EntityManager manager, ResultSet res, Class<Enum<?>> type, String columnName)
        throws SQLException
    {
        final String dbValue = res.getString(columnName);
        if (StringUtils.isBlank(dbValue))
        {
            return null;
        }
        try
        {
            return Enum.valueOf((Class<? extends Enum>) type, dbValue);
        }
        catch (IllegalArgumentException e)
        {
            throw new ActiveObjectsException("Could not find enum value for '" + type + "' corresponding to database value '" + dbValue + "'");
        }
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Enum<?> value, int jdbcType) throws SQLException
    {
        stmt.setString(index, value.name());
    }

    //@Override
    //public Enum<?> parse(String input)
    //{
    //    try
    //    {
    //        return Integer.parseInt(value);
    //    }
    //    catch (NumberFormatException e)
    //    {
    //        throw new ActiveObjectsConfigurationException("Could not parse '" + value + "' as an integer to match enum ordinal");
    //    }
    // }
}