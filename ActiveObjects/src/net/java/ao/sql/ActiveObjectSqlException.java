package net.java.ao.sql;

import net.java.ao.ActiveObjectsException;

import java.sql.SQLException;

/**
 * A generic runtime exception to wrap {@link java.sql.SQLException}
 */
public class ActiveObjectSqlException extends ActiveObjectsException
{
    public ActiveObjectSqlException(SQLException cause)
    {
        super(cause);
    }

    public SQLException getSqlException()
    {
        return (SQLException) getCause();
    }
}
