package net.java.ao.test;

import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.event.EventManager;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

import static junit.framework.Assert.assertEquals;
import static net.java.ao.Common.closeQuietly;

/**
 *
 */
@RunWith(ActiveObjectsJUnitRunner.class)
public abstract class ActiveObjectsIntegrationTest
{
    protected EntityManager entityManager;

    protected final <T> T checkSqlExecuted(Callable<T> callable) throws Exception
    {
        return checkSql(true, callable);
    }

    protected final <T> T checkSqlNotExecuted(Callable<T> callable) throws Exception
    {
        return checkSql(false, callable);
    }

    private <T> T checkSql(boolean executed, Callable<T> callable) throws Exception
    {
        final EventManager eventManager = entityManager.getEventManager();
        final SqlTracker sqlTracker = new SqlTracker();
        try
        {
            eventManager.register(sqlTracker);
            final T t = callable.call();
            assertEquals(executed, sqlTracker.isSqlExecuted());
            return t;
        }
        finally
        {
            eventManager.unregister(sqlTracker);
        }
    }

    protected final <E extends RawEntity<?>> E checkSqlExecutedWhenSaving(final E entity) throws Exception
    {
        return checkSqlExecuted(new Callable<E>()
        {
            public E call() throws Exception
            {
                entity.save();
                return entity;
            }
        });
    }

    protected final void executeUpdate(final String sql, UpdateCallback callback) throws Exception
    {
        Connection connection = null;
        PreparedStatement statement = null;
        try
        {
            connection = entityManager.getProvider().getConnection();
            statement = connection.prepareStatement(sql);
            callback.setParameters(statement);
            statement.executeUpdate();
        }
        finally
        {
            closeQuietly(statement);
            closeQuietly(connection);
        }
    }

    protected final void executeStatement(final String sql, StatementCallback callback) throws Exception
    {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try
        {
            connection = entityManager.getProvider().getConnection();
            statement = connection.prepareStatement(sql);
            callback.setParameters(statement);
            resultSet = statement.executeQuery();
            callback.processResult(resultSet);
        }
        finally
        {
            closeQuietly(resultSet);
            closeQuietly(statement);
            closeQuietly(connection);
        }
    }

    protected interface StatementCallback
    {
        void setParameters(PreparedStatement statement) throws Exception;

        void processResult(ResultSet resultSet) throws Exception;
    }

    protected abstract class UpdateCallback implements StatementCallback
    {
        public final void processResult(ResultSet resultSet) throws Exception
        {
        }
    }

    protected final String getTableName(Class<? extends RawEntity<?>> entityType)
    {
        return getTableName(entityType, true);
    }

    /**
     * Get the table name of the given class entity
     *
     * @param entityType the class of the entity
     * @param escape whether or not to escape the table name
     * @return the table name
     */
    protected final String getTableName(Class<? extends RawEntity<?>> entityType, boolean escape)
    {
        final String tableName = entityManager.getTableNameConverter().getName(entityType);
        return escape ? escapeKeyword(tableName) : tableName;
    }

    protected final String escapeKeyword(String keyword)
    {
        return entityManager.getProvider().processID(keyword);
    }
}
