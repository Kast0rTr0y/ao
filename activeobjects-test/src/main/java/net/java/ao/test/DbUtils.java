package net.java.ao.test;

import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;

import static net.java.ao.sql.SqlUtils.closeQuietly;
import static org.junit.Assert.assertEquals;

public final class DbUtils
{
    private static final Logger logger = LoggerFactory.getLogger(DbUtils.class);

    public static boolean isOracle(EntityManager em)
    {
        return em.getProvider() instanceof OracleDatabaseProvider;
    }

    public static boolean isHsql(EntityManager em)
    {
        return em.getProvider() instanceof HSQLDatabaseProvider;
    }

    public static <T> T checkSqlExecuted(EntityManager em, Callable<T> callable) throws Exception
    {
        return checkSql(em, true, callable);
    }

    public static <T> T checkSqlNotExecuted(EntityManager em, Callable<T> callable) throws Exception
    {
        return checkSql(em, false, callable);
    }

    public static <E extends RawEntity<?>> E checkSqlExecutedWhenSaving(EntityManager em, final E entity) throws Exception
    {
        return checkSqlExecuted(em, new Callable<E>()
        {
            public E call() throws Exception
            {
                entity.save();
                return entity;
            }
        });
    }

    public static void executeUpdate(EntityManager em, String sql, UpdateCallback callback) throws Exception
    {
        Connection connection = null;
        PreparedStatement statement = null;
        try
        {
            connection = em.getProvider().getConnection();
            statement = connection.prepareStatement(sql);
            logger.debug(sql);
            callback.setParameters(statement);
            statement.executeUpdate();
        }
        finally
        {
            closeQuietly(statement, connection);
        }
    }

    public static void executeStatement(EntityManager em, String sql, StatementCallback callback) throws Exception
    {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try
        {
            connection = em.getProvider().getConnection();
            statement = connection.prepareStatement(sql);
            logger.debug(sql);
            callback.setParameters(statement);
            resultSet = statement.executeQuery();
            callback.processResult(resultSet);
        }
        finally
        {
            closeQuietly(resultSet, statement, connection);
        }
    }

    public static interface StatementCallback
    {
        void setParameters(PreparedStatement statement) throws Exception;

        void processResult(ResultSet resultSet) throws Exception;
    }

    public static abstract class UpdateCallback implements StatementCallback
    {
        public final void processResult(ResultSet resultSet) throws Exception
        {
        }
    }

    private static <T> T checkSql(EntityManager em, boolean executed, Callable<T> callable) throws Exception
    {
        final DatabaseProvider provider = em.getProvider();
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
}
