package net.java.ao.event.sql;

/**
 * This represent an SQL statement that is about to be issued.
 */
public class SqlEvent
{
    private final String sql;

    public SqlEvent(String sql)
    {
        this.sql = sql;
    }

    public String getSql()
    {
        return sql;
    }

    @Override
    public String toString()
    {
        return sql;
    }
}
