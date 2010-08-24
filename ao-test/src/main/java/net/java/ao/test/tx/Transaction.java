package net.java.ao.test.tx;

import net.java.ao.DelegateConnection;
import net.java.ao.EntityManager;

import java.sql.Connection;
import java.sql.SQLException;

public final class Transaction
{
    private final EntityManager entityManager;
    private Connection connection = null;

    public Transaction(EntityManager entityManager)
    {
        this.entityManager = entityManager;
    }

    public void start()
    {
        try
        {
            connection = entityManager.getProvider().getConnection();
            setCloseable(connection, false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connection.setAutoCommit(false);
        }
        catch (SQLException e)
        {
            throw new TransactionException(e);
        }
    }

    public void rollback()
    {
        try
        {
            if (connection != null && !connection.isClosed())
            {
                connection.rollback();
                setCloseable(connection, true);
                connection.close();
            }
        }
        catch (SQLException e)
        {
            throw new TransactionException(e);
        }
    }

    private void setCloseable(Connection connection, boolean closeable)
    {
        if (connection instanceof DelegateConnection)
            ((DelegateConnection) connection).setCloseable(closeable);
    }
}
