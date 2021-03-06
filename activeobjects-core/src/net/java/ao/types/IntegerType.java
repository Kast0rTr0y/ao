package net.java.ao.types;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;

final class IntegerType extends AbstractLogicalType<Integer> {
    public IntegerType() {
        super("Integer",
                new Class<?>[]{Integer.class, int.class},
                INTEGER, new Integer[]{INTEGER, NUMERIC});
    }

    @Override
    public boolean isAllowedAsPrimaryKey() {
        return true;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Integer value, int jdbcType) throws SQLException {
        stmt.setInt(index, value);
    }

    @Override
    public Integer pullFromDatabase(EntityManager manager, ResultSet res, Class<Integer> type, int columnIndex) throws SQLException {
        return preserveNull(res, res.getInt(columnIndex));
    }

    @Override
    public Integer pullFromDatabase(EntityManager manager, ResultSet res, Class<Integer> type, String columnName)
            throws SQLException {
        return preserveNull(res, res.getInt(columnName));
    }

    @Override
    public Integer parse(String input) {
        return StringUtils.isBlank(input) ? null : Integer.parseInt(input);
    }


}
