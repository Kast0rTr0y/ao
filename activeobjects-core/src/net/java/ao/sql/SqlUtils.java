package net.java.ao.sql;

import com.google.common.base.Function;
import net.java.ao.Common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

public final class SqlUtils
{
    public static final Pattern WHERE_CLAUSE = Pattern.compile("(\\w+)(?=\\s*((=|!=|>|<|<>|<=|>=|(?<!(NOT\\s{1,10}))LIKE|(?<!(NOT\\s{1,10}))like|(?<!(NOT\\s{1,10}))BETWEEN|(?<!(NOT\\s{1,10}))between|IS|is|(?<!((IS|AND)\\s{1,10}))NOT|(?<!(NOT\\s{1,10}))IN|(?<!(is\\s{1,10}))not|(?<!(not\\s{1,10}))in)(\\s|\\()))");
    public static final Pattern ON_CLAUSE = Pattern.compile("(?:(\\w+)\\.)?(?:(\\w+)\\.)?(\\w+)(\\s*=\\s*)(?:(\\w+)\\.)?(?:(\\w+)\\.)?(\\w+)");
    public static final Pattern ORDER_CLAUSE = Pattern.compile("(?:(\\w+)\\.)?(\\w+)(?:\\s*(ASC|DESC))?");
    public static final Pattern GROUP_BY_CLAUSE = Pattern.compile("(?:(\\w+)\\.)?(\\w+)");

    private SqlUtils()
    {
    }

    public static String processWhereClause(String where, Function<String, String> processor)
    {
        final Matcher matcher = WHERE_CLAUSE.matcher(where);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find())
        {
            matcher.appendReplacement(sb, processor.apply(matcher.group()));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String processOnClause(String on, Function<String, String> processor)
    {
        final Matcher matcher = ON_CLAUSE.matcher(on);
        checkState(matcher.matches());
        final StringBuilder sb = new StringBuilder();
        if (matcher.group(1) != null)
        {
            sb.append(matcher.group(1)).append(".");
            if (matcher.group(2) != null)
            {
                sb.append(processor.apply(matcher.group(2))).append(".");
            }
        }
        else if (matcher.group(2) != null)
        {
            sb.append(matcher.group(2)).append(".");
        }
        sb.append(processor.apply(matcher.group(3)));
        sb.append(matcher.group(4));
        if (matcher.group(5) != null)
        {
            sb.append(matcher.group(5)).append(".");
            if (matcher.group(6) != null)
            {
                sb.append(processor.apply(matcher.group(6))).append(".");
            }
        }
        else if (matcher.group(6) != null)
        {
            sb.append(matcher.group(6)).append(".");
        }
        sb.append(processor.apply(matcher.group(7)));
        return sb.toString();
    }
    
    public static String processGroupByClause(String groupBy, Function<String, String> processor)
    {
        final Matcher matcher = GROUP_BY_CLAUSE.matcher(groupBy);
        final StringBuffer sql = new StringBuffer();
        while(matcher.find())
        {
            final StringBuilder repl = new StringBuilder();

            // $1 signifies the (optional) table name to potentially quote
            if (matcher.group(1) != null)
            {
                repl.append(processor.apply("$1"));
                repl.append(".");
            }

            // $2 signifies the (mandatory) column name to potentially quote
            repl.append(processor.apply("$2"));

            matcher.appendReplacement(sql, repl.toString());
        }
        matcher.appendTail(sql);
        return sql.toString();
    }

    @Deprecated
    public static void closeQuietly(Connection connection)
    {
        Common.closeQuietly(connection);
    }

    @Deprecated
    public static void closeQuietly(Statement statement)
    {
        Common.closeQuietly(statement);
    }

    @Deprecated
    public static void closeQuietly(ResultSet resultSet)
    {
        Common.closeQuietly(resultSet);
    }

    @Deprecated
    public static void closeQuietly(Statement st, Connection c)
    {
        closeQuietly(st);
        closeQuietly(c);
    }

    @Deprecated
    public static void closeQuietly(ResultSet rs, Statement st, Connection c)
    {
        closeQuietly(rs);
        closeQuietly(st, c);
    }
}