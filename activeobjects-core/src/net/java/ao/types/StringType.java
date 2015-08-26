package net.java.ao.types;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.CLOB;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.VARCHAR;

final class StringType extends AbstractLogicalType<String> {
    public static final int DEFAULT_LENGTH = 255;

    public StringType() {
        super("String",
                new Class<?>[]{String.class},
                VARCHAR, new Integer[]{VARCHAR, NVARCHAR, LONGVARCHAR, LONGNVARCHAR, CLOB, NCLOB});
    }

    @Override
    public boolean isAllowedAsPrimaryKey() {
        return true;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, String value, int jdbcType) throws SQLException {
        stmt.setString(index, value);
    }

    @Override
    public String pullFromDatabase(EntityManager manager, ResultSet res, Class<String> type, String columnName) throws SQLException {
        return res.getString(columnName);
    }

    @Override
    public String parse(String input) {
        return input;
    }

    @Override
    public String parseDefault(String input) {
        if (StringUtils.isBlank(input)) {
            throw new IllegalArgumentException("Empty strings are not supported on all databases. Therefore is not supported by Active Objects.");
        }
        return input;
    }
}