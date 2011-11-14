package net.java.ao.test;

import com.google.common.base.Predicate;
import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.test.converters.DynamicFieldNameConverter;
import net.java.ao.test.converters.DynamicTableNameConverter;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.runner.RunWith;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.*;
import static junit.framework.Assert.*;
import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 *
 */
@RunWith(ActiveObjectsJUnitRunner.class)
@NameConverters(table = DynamicTableNameConverter.class, field = DynamicFieldNameConverter.class)
@Jdbc(DynamicJdbcConfiguration.class)
public abstract class ActiveObjectsIntegrationTest
{
    protected EntityManager entityManager;

    protected final boolean isOracle()
    {
        return entityManager.getProvider() instanceof OracleDatabaseProvider;
    }

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
        final DatabaseProvider provider = entityManager.getProvider();
        final SqlTracker sqlTracker = new SqlTracker();
        try
        {
            provider.addSqlListener(sqlTracker);
            final T t = callable.call();
            assertEquals(executed, sqlTracker.isSqlExecuted());
            return t;
        }
        finally
        {
            provider.removeSqlListener(sqlTracker);
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
            closeQuietly(statement, connection);
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
            closeQuietly(resultSet, statement, connection);
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
        final String tableName = entityManager.getNameConverters().getTableNameConverter().getName(entityType);
        return escape ? entityManager.getProvider().withSchema(tableName) : tableName;
    }

    protected final String getFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return entityManager.getNameConverters().getFieldNameConverter().getName(findMethod(entityType, methodName));
    }

    protected final String getPolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return entityManager.getNameConverters().getFieldNameConverter().getPolyTypeName(findMethod(entityType, methodName));
    }

    protected final String escapeFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return escapeKeyword(getFieldName(entityType, methodName));
    }

    protected final String escapePolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return escapeKeyword(getPolyFieldName(entityType, methodName));
    }

    private Method findMethod(Class<? extends RawEntity<?>> entityType, final String methodName)
    {
        return find(newArrayList(entityType.getMethods()), new Predicate<Method>()
        {
            @Override
            public boolean apply(Method m)
            {
                return m.getName().equals(methodName);
            }
        });
    }

    protected final String escapeKeyword(String keyword)
    {
        return entityManager.getProvider().processID(keyword);
    }
}
