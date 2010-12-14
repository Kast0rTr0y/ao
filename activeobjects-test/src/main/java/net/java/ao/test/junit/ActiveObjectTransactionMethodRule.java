package net.java.ao.test.junit;

import net.java.ao.EntityManager;
import net.java.ao.builder.EntityManagerBuilder;
import net.java.ao.builder.EntityManagerBuilderWithDatabaseProperties;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.test.tx.Transaction;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ActiveObjectTransactionMethodRule implements MethodRule
{
    private static final Map<JdbcConfiguration, Class<? extends DatabaseUpdater>> DATABASES =
            new HashMap<JdbcConfiguration, Class<? extends DatabaseUpdater>>();

    private final Object test;
    private final JdbcConfiguration jdbc;
    private final boolean withIndex;

    private EntityManager entityManager;
    private Transaction transaction;
    private File indexDirectory;

    public ActiveObjectTransactionMethodRule(Object test, JdbcConfiguration jdbc, boolean withIndex)
    {
        this.test = test;
        this.jdbc = jdbc;
        this.withIndex = withIndex;
    }

    public final Statement apply(final Statement base, final FrameworkMethod method, final Object target)
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before(method);
                try
                {
                    base.evaluate();
                }
                finally
                {
                    after(method);
                }
            }
        };
    }

    protected void before(FrameworkMethod method) throws Throwable
    {
        createIndexDir();
        entityManager = createEntityManager();
        injectEntityManager();
        updateDatabase();
        if (useTransaction(method))
        {
            transaction = createTransaction();
            transaction.start();
        }
    }

    private void createIndexDir()
    {
        try
        {
            indexDirectory = File.createTempFile("ao_test", "index");
            indexDirectory.delete();
            indexDirectory.mkdirs();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void after(FrameworkMethod method)
    {
        if (useTransaction(method))
        {
            transaction.rollback();
            transaction = null;
        }
        else
        {
            DATABASES.remove(jdbc.getClass()); // make sure that the next test gets a clean database
        }

        entityManager = null;
        removeIndexDir();
    }

    private boolean useTransaction(FrameworkMethod method)
    {
        return !method.getMethod().isAnnotationPresent(NonTransactional.class);
    }

    private void removeIndexDir()
    {
        removeFile(indexDirectory);
    }

    private void removeFile(File file)
    {
        if (file == null || !file.exists())
        {
            return;
        }

        if (file.isFile())
        {
            file.delete();
        }
        else
        {
            for (File f : file.listFiles())
            {
                removeFile(f);
            }
            file.delete(); // now we can delete the empty directory
        }
    }

    private Transaction createTransaction()
    {
        return new Transaction(entityManager);
    }

    private EntityManager createEntityManager()
    {
        EntityManagerBuilderWithDatabaseProperties entityManagerBuilder = EntityManagerBuilder.url(jdbc.getUrl()).username(jdbc.getUsername()).password(jdbc.getPassword()).auto();
        return withIndex ? entityManagerBuilder.withIndex(indexDirectory).build() : entityManagerBuilder.build();
    }

    private void injectEntityManager()
    {
        final Field field = getEntityManagerField(getTestClass());
        final boolean isFieldAccessible = field.isAccessible();
        try
        {
            field.setAccessible(true);
            try
            {
                field.set(test, entityManager);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }
        finally
        {
            field.setAccessible(isFieldAccessible);
        }
    }

    private void updateDatabase() throws Exception
    {
        if (getTestClass().isAnnotationPresent(Data.class))
        {
            final Class<? extends DatabaseUpdater> databaseUpdater = getTestClass().getAnnotation(Data.class).value();
            if (!DATABASES.containsKey(jdbc)
                    || !DATABASES.get(jdbc).equals(databaseUpdater))
            {
                entityManager.migrate(); // empty the database
                newInstance(databaseUpdater).update(entityManager);
                DATABASES.put(jdbc, databaseUpdater);
            }
        }
    }

    private <T> T newInstance(Class<T> aClass)
    {
        try
        {
            return aClass.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Class<?> getTestClass()
    {
        return test.getClass();
    }

    private Field getEntityManagerField(Class<?> aClass)
    {
        for (Field field : aClass.getDeclaredFields())
        {
            if (field.getType().equals(EntityManager.class))
            {
                return field;
            }
        }

        if (!aClass.getSuperclass().equals(Object.class))
        {
            return getEntityManagerField(aClass.getSuperclass());
        }

        return null;
    }
}
