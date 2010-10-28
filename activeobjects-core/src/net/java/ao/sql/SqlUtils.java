package net.java.ao.sql;

import net.java.ao.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 */
public final class SqlUtils
{
    private SqlUtils()
    {
    }

    public static void closeQuietly(Connection connection)
    {
        Common.closeQuietly(connection);
    }

    public static void closeQuietly(Statement statement)
    {
        Common.closeQuietly(statement);
    }

    public static void closeQuietly(ResultSet resultSet)
    {
        Common.closeQuietly(resultSet);
    }
}
