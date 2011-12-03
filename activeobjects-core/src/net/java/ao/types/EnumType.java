package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;

import static java.sql.Types.INTEGER;
import static net.java.ao.util.EnumUtils.values;

final class EnumType extends AbstractLogicalType<Enum<?>>
{
    public EnumType()
    {
        super("Enum",
              new Class<?>[] { Enum.class },
              INTEGER, new Integer[] { });
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }
    
    @Override
    public Enum<?> pullFromDatabase(EntityManager manager, ResultSet res, Class<Enum<?>> type, String columnName)
        throws SQLException
    {
        final int dbValue = res.getInt(columnName);
        try
        {
            return findEnum(type, dbValue);
        }
        catch (NoSuchElementException e)
        {
            throw new ActiveObjectsException("Could not find enum value for '" + type + "' corresponding to database value '" + dbValue + "'");
        }
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Enum<?> value, int jdbcType) throws SQLException
    {
        stmt.setInt(index, value.ordinal());
    }

    // Parsing won't be possible until we know the desired enum type at the time we're parsing the value.
    // Java won't let us cast an integer to an enum.
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
    
    private Enum<?> findEnum(Class<? extends Enum<?>> type, final int dbValue)
    {
        return Iterables.find(values(type), new Predicate<Enum>()
        {
            @Override
            public boolean apply(Enum e)
            {
                return e.ordinal() == dbValue;
            }
        });
    }
}