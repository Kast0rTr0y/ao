package net.java.ao.test;

import net.java.ao.event.EventListener;
import net.java.ao.event.sql.SqlEvent;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public final class SqlTracker
{
    private final List<SqlEvent> sqlStatements = new LinkedList<SqlEvent>();

    @EventListener
    public void onSql(SqlEvent sql)
    {
        sqlStatements.add(sql);
    }

    public boolean isSqlExecuted()
    {
        return !sqlStatements.isEmpty();
    }
}
