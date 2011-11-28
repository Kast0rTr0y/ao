package net.java.ao.test;

import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.test.converters.DynamicFieldNameConverter;
import net.java.ao.test.converters.DynamicTableNameConverter;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.converters.UpperCaseFieldNameConverter;
import net.java.ao.test.converters.UpperCaseTableNameConverter;
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.MySql;
import net.java.ao.test.jdbc.Oracle;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

/**
 *
 */
@RunWith(ActiveObjectsJUnitRunner.class)
@NameConverters(table = UpperCaseTableNameConverter.class, field = UpperCaseFieldNameConverter.class)
@Jdbc(Oracle.class)
public abstract class ActiveObjectsIntegrationTest
{
    protected EntityManager entityManager;

    protected final boolean isOracle()
    {
        return DbUtils.isOracle(entityManager);
    }

    protected final <T> T checkSqlExecuted(Callable<T> callable) throws Exception
    {
        return DbUtils.checkSqlExecuted(entityManager, callable);
    }

    protected final <T> T checkSqlNotExecuted(Callable<T> callable) throws Exception
    {
        return DbUtils.checkSqlNotExecuted(entityManager, callable);
    }

    protected final <E extends RawEntity<?>> E checkSqlExecutedWhenSaving(final E entity) throws Exception
    {
        return DbUtils.checkSqlExecutedWhenSaving(entityManager, entity);
    }

    protected final void executeUpdate(final String sql, DbUtils.UpdateCallback callback) throws Exception
    {
        DbUtils.executeUpdate(entityManager, sql, callback);
    }

    protected final void executeStatement(final String sql, DbUtils.StatementCallback callback) throws Exception
    {
        DbUtils.executeStatement(entityManager, sql, callback);
    }

    protected final String getTableName(Class<? extends RawEntity<?>> entityType)
    {
        return EntityUtils.getTableName(entityManager, entityType);
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
        return EntityUtils.getTableName(entityManager, entityType, escape);
    }

    protected final String getFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return EntityUtils.getFieldName(entityManager, entityType, methodName);
    }

    protected final String getPolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return EntityUtils.getPolyFieldName(entityManager, entityType, methodName);
    }

    protected final String escapeFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return EntityUtils.escapeFieldName(entityManager, entityType, methodName);
    }

    protected final String escapePolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName)
    {
        return EntityUtils.escapePolyFieldName(entityManager, entityType, methodName);
    }

    protected final String escapeKeyword(String keyword)
    {
        return EntityUtils.escapeKeyword(entityManager, keyword);
    }
}
