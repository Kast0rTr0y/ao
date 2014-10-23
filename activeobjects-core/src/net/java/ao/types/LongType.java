package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static java.sql.Types.BIGINT;
import static java.sql.Types.NUMERIC;

final class LongType extends AbstractLogicalType<Long>
{
    public LongType()
    {
        super("Long",
              new Class<?>[] { Long.class, long.class },
              BIGINT, new Integer[] { BIGINT, NUMERIC });
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Long value, int jdbcType) throws SQLException
    {
        stmt.setLong(index, value);
    }

    @Override
    public Long pullFromDatabase(EntityManager manager, ResultSet res, Class<Long> type, int columnIndex) throws SQLException
    {
        final Long value = res.getLong(columnIndex);
        
        return getLong(res, value);
    }

    @Override
    public Long pullFromDatabase(EntityManager manager, ResultSet res, Class<Long> type, String columnName) throws SQLException
    {
        final Long value = res.getLong(columnName);
        
        return getLong(res, value);
    }

    private Long getLong(ResultSet res, Long value) throws SQLException
    {
        return res.wasNull() ? null : value;
    }

    @Override
    public Long parse(String input)
    {
        return StringUtils.isBlank(input) ? null : Long.parseLong(input);
    }
}
