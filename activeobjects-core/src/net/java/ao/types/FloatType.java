package net.java.ao.types;

import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.REAL;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static java.sql.Types.FLOAT;
import static java.sql.Types.NUMERIC;

final class FloatType extends AbstractLogicalType<Float>
{
    public FloatType()
    {
        super("Float",
              new Class<?>[] { Float.class, float.class },
              FLOAT, new Integer[] { FLOAT, REAL, NUMERIC });
    }

    @Override
    public Float pullFromDatabase(EntityManager manager, ResultSet res, Class<Float> type, String columnName)
        throws SQLException
    {
        return res.getFloat(columnName);
    }
    
    @Override
    public Float parse(String input)
    {
        return StringUtils.isBlank(input) ? null : Float.parseFloat(input);
    }
}
