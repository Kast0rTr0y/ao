package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.BIT;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static java.sql.Types.BOOLEAN;
import static java.sql.Types.NUMERIC;

final class BooleanType extends AbstractLogicalType<Boolean>
{
    public BooleanType()
    {
        super("Boolean",
              new Class<?>[] { Boolean.class, boolean.class },
              BOOLEAN, new Integer[] { BOOLEAN, BIT, NUMERIC });
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Boolean value, int jdbcType) throws SQLException
    {
        manager.getProvider().putBoolean(stmt, index, value);
    }

    @Override
    public Boolean pullFromDatabase(EntityManager manager, ResultSet res, Class<Boolean> type, String columnName)
        throws SQLException
    {
        return preserveNull(res, res.getBoolean(columnName));
    }
    
    @Override
    public Boolean parse(String input)
    {
        return StringUtils.isBlank(input) ? null : Boolean.parseBoolean(input);
    }

    @Override
    public boolean valueEquals(Object a, Object b)
    {
        if (a instanceof Number)
        {
            if (b instanceof Boolean)
            {
                return (((Number) a).intValue() == 1) == ((Boolean) b).booleanValue();
            }
        }
        else if (a instanceof Boolean)
        {
            if (b instanceof Number)
            {
                return (((Number) b).intValue() == 1) == ((Boolean) a).booleanValue();
            }
        }

        return a.equals(b);
    }
}
