package net.java.ao.sql;

import com.google.common.base.Function;
import net.java.ao.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 *
 */
public final class SqlUtils
{
    public static final Pattern WHERE_CLAUSE = Pattern.compile("(\\w+)(?=\\s*((=|>|<|<>|LIKE|like|BETWEEN|between|IS|is|(?<!(IS\\s{1,10}))NOT|(?<!(NOT\\s{1,10}))IN|(?<!(is\\s{1,10}))not|(?<!(not\\s{1,10}))in)(\\s|\\()))");
    public static final Pattern ON_CLAUSE = Pattern.compile("(?:(\\w+\\.)?(\\w+))(?:\\s*=\\s*)(?:(\\w+\\.)?(\\w+))");
    public static final Pattern ORDER_CLAUSE = Pattern.compile("(\\w+)(?=\\s*(ASC|DESC))?");
    public static final Pattern GROUP_BY_CLAUSE = Pattern.compile("(\\w+)(?:,(\\w+))*");

    private SqlUtils()
    {
    }

    public static String processWhereClause(String where, Function<String, String> fieldNameProcessor)
    {
        return WHERE_CLAUSE.matcher(where).replaceAll(fieldNameProcessor.apply("$1"));
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
