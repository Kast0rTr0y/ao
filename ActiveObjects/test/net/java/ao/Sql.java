package net.java.ao;

import net.java.ao.event.EventListener;
import net.java.ao.event.EventManager;
import net.java.ao.event.sql.SqlEvent;

import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public final class Sql
{
    private final EventManager eventManager;

    public Sql(EventManager eventManager)
    {
        this.eventManager = eventManager;
    }

    public <T> T checkExecuted(Command<T> command) throws Exception
    {
        return check(command, true, "No SQL was executed!");
    }

    public <T> T checkNotExecuted(Command<T> command) throws Exception
    {
        return check(command, false, "Some SQL statements were executed!");
    }

    private <T> T check(Command<T> command, boolean executed, String message) throws Exception
    {
        final SqlTracker sqlTracker = new SqlTracker();
        try
        {
            eventManager.register(sqlTracker);
            final T t = command.run();
            assertEquals(message, executed, sqlTracker.isSqlExecuted());
            return t;
        }
        finally
        {
            eventManager.unregister(sqlTracker);
        }
    }

    public static class SqlTracker
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
}
