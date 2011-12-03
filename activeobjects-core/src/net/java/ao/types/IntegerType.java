package net.java.ao.types;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;

final class IntegerType extends AbstractLogicalType<Integer>
{
    public IntegerType()
    {
        super("Integer",
              new Class<?>[] { Integer.class, int.class },
              INTEGER, new Integer[] { INTEGER, NUMERIC });
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }

    @Override
    public Integer pullFromDatabase(EntityManager manager, ResultSet res, Class<Integer> type, String columnName)
        throws SQLException
    {
        return res.getInt(columnName);
    }
    
    @Override
    public Integer parse(String input)
    {
        return StringUtils.isBlank(input) ? null : Integer.parseInt(input);
    }
}
