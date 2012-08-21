package net.java.ao.test.junit;

import net.java.ao.DelegateConnection;
import net.java.ao.EntityManager;
import net.java.ao.builder.EntityManagerBuilder;
import net.java.ao.builder.EntityManagerBuilderWithDatabaseProperties;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.Table;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.test.jdbc.NullDatabase;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 *
 */
public class ActiveObjectTransactionMethodRule implements MethodRule
{
    private static final Map<JdbcConfiguration, DatabaseConfiguration> DATABASES = new HashMap<JdbcConfiguration, DatabaseConfiguration>();

    private final Object test;
    private final JdbcConfiguration jdbc;
    private final boolean withIndex;
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final SequenceNameConverter sequenceNameConverter;
    private final TriggerNameConverter triggerNameConverter;
    private final IndexNameConverter indexNameConverter;

    private EntityManager entityManager;
    private File indexDirectory;

    public ActiveObjectTransactionMethodRule(Object test,
                                             JdbcConfiguration jdbc,
                                             boolean withIndex,
                                             TableNameConverter tableNameConverter,
                                             FieldNameConverter fieldNameConverter,
                                             SequenceNameConverter sequenceNameConverter,
                                             TriggerNameConverter triggerNameConverter,
                                             IndexNameConverter indexNameConverter)
    {
        this.test = test;
        this.jdbc = jdbc;
        this.withIndex = withIndex;
        this.tableNameConverter = tableNameConverter;
        this.fieldNameConverter = fieldNameConverter;
        this.sequenceNameConverter = sequenceNameConverter;
        this.triggerNameConverter = triggerNameConverter;
        this.indexNameConverter = indexNameConverter;
    }

    public final Statement apply(final Statement base, final FrameworkMethod method, final Object target)
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                before(method);
                final boolean useTransaction = useTransaction(method);
                Connection c = null;
                try
                {
                    if (useTransaction)
                    {
                        c = entityManager.getProvider().startTransaction();
                    }

                    base.evaluate();
                }
                finally
                {
                    if (useTransaction && c != null)
                    {
                        entityManager.getProvider().rollbackTransaction(c);

                        // make it closeable
                        if (c instanceof DelegateConnection)
                            ((DelegateConnection) c).setCloseable(true);
                        // close it
                        closeQuietly(c);
                    }
                    entityManager.flushAll();
                    after(method);
                }
            }
        };
    }

    protected void before(FrameworkMethod method) throws Throwable
    {
        if (withIndex) createIndexDir();
        entityManager = createEntityManagerAndUpdateDatabase();
        injectEntityManager();
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
        if (!useTransaction(method))
        {
            DATABASES.remove(jdbc); // make sure that the next test gets a clean database
        }
        entityManager = null;
        if (withIndex) removeIndexDir();
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

    private EntityManager createEntityManagerAndUpdateDatabase() throws Exception
    {
        final Class<? extends DatabaseUpdater> databaseUpdater = isDataAnnotationPresent() ? getDataAnnotationValue() : getDataAnnotationDefaultValue();
        final DatabaseConfiguration dbConfiguration = newDatabaseConfiguration(databaseUpdater);
        final EntityManager entityManager;
        if (databaseUpdater == NullDatabase.class)
        {
            entityManager = createEntityManager();
            entityManager.migrateAggressively(); // empty the database
            DATABASES.remove(jdbc);
        }
        else if (!DATABASES.containsKey(jdbc) || !DATABASES.get(jdbc).equals(dbConfiguration) || withIndex)
        {
            entityManager = createEntityManager();
            entityManager.migrateAggressively(); // empty the database
            newInstance(databaseUpdater).update(entityManager);
            dbConfiguration.setEntityManager(entityManager);
            DATABASES.put(jdbc, dbConfiguration);
        }
        else
        {
            entityManager = DATABASES.get(jdbc).getEntityManager();
        }

        return entityManager;
    }

    private DatabaseConfiguration newDatabaseConfiguration(Class<? extends DatabaseUpdater> databaseUpdater)
    {
        final Class<? extends TableNameConverter> tableNameConverterClass = (Class<? extends TableNameConverter>) getClass(tableNameConverter);
        final Class<? extends FieldNameConverter> fieldNameConverterClass = (Class<? extends FieldNameConverter>) getClass(fieldNameConverter);
        final Class<? extends SequenceNameConverter> sequenceNameConverterClass = (Class<? extends SequenceNameConverter>) getClass(sequenceNameConverter);
        final Class<? extends TriggerNameConverter> triggerNameConverterClass = (Class<? extends TriggerNameConverter>) getClass(triggerNameConverter);
        final Class<? extends IndexNameConverter> indexNameConverterClass = (Class<? extends IndexNameConverter>) getClass(indexNameConverter);
        return new DatabaseConfiguration(databaseUpdater, tableNameConverterClass, fieldNameConverterClass, sequenceNameConverterClass, triggerNameConverterClass, indexNameConverterClass, withIndex);
    }

    private Class<?> getClass(Object o)
    {
        return o != null ? o.getClass() : null;
    }

    private EntityManager createEntityManager()
    {
        EntityManagerBuilderWithDatabaseProperties entityManagerBuilder = EntityManagerBuilder.url(jdbc.getUrl()).username(jdbc.getUsername()).password(jdbc.getPassword()).schema(jdbc.getSchema()).auto();

        if (tableNameConverter != null)
        {
            entityManagerBuilder = entityManagerBuilder.tableNameConverter(tableNameConverter);
        }
        if (fieldNameConverter != null)
        {
            entityManagerBuilder = entityManagerBuilder.fieldNameConverter(fieldNameConverter);
        }
        if (sequenceNameConverter != null)
        {
            entityManagerBuilder = entityManagerBuilder.sequenceNameConverter(sequenceNameConverter);
        }
        if (triggerNameConverter != null)
        {
            entityManagerBuilder = entityManagerBuilder.triggerNameConverter(triggerNameConverter);
        }
        if (indexNameConverter != null)
        {
            entityManagerBuilder = entityManagerBuilder.indexNameConverter(indexNameConverter);
        }
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

    private Class<? extends DatabaseUpdater> getDataAnnotationDefaultValue() throws NoSuchMethodException
    {
        @Data
        final class C
        {
        }
        return C.class.getAnnotation(Data.class).value();
    }

    private Class<? extends DatabaseUpdater> getDataAnnotationValue()
    {
        return getTestClass().getAnnotation(Data.class).value();
    }

    private boolean isDataAnnotationPresent()
    {
        return getTestClass().isAnnotationPresent(Data.class);
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

    private static final class DatabaseConfiguration
    {
        private final Class<? extends DatabaseUpdater> databaseUpdaterClass;
        private final Class<? extends TableNameConverter> tableNameConverterClass;
        private final Class<? extends FieldNameConverter> fieldNameConverterClass;
        private final Class<? extends SequenceNameConverter> sequenceNameConverterClass;
        private final Class<? extends TriggerNameConverter> triggerNameConverterClass;
        private final Class<? extends IndexNameConverter> indexNameConverterClass;
        private final boolean withIndex;

        private EntityManager entityManager;

        public DatabaseConfiguration(Class<? extends DatabaseUpdater> databaseUpdaterClass,
                                     Class<? extends TableNameConverter> tableNameConverterClass,
                                     Class<? extends FieldNameConverter> fieldNameConverterClass,
                                     Class<? extends SequenceNameConverter> sequenceNameConverterClass,
                                     Class<? extends TriggerNameConverter> triggerNameConverterClass,
                                     Class<? extends IndexNameConverter> indexNameConverterClass,
                                     boolean withIndex)
        {
            this.databaseUpdaterClass = databaseUpdaterClass;
            this.tableNameConverterClass = tableNameConverterClass;
            this.fieldNameConverterClass = fieldNameConverterClass;
            this.sequenceNameConverterClass = sequenceNameConverterClass;
            this.triggerNameConverterClass = triggerNameConverterClass;
            this.indexNameConverterClass = indexNameConverterClass;
            this.withIndex = withIndex;
        }

        public void setEntityManager(EntityManager entityManager)
        {
            this.entityManager = entityManager;
        }

        public EntityManager getEntityManager()
        {
            return entityManager;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final DatabaseConfiguration that = (DatabaseConfiguration) o;

            if (withIndex != that.withIndex)
            {
                return false;
            }
            if (databaseUpdaterClass != null ? !databaseUpdaterClass.equals(that.databaseUpdaterClass) : that.databaseUpdaterClass != null)
            {
                return false;
            }
            if (fieldNameConverterClass != null ? !fieldNameConverterClass.equals(that.fieldNameConverterClass) : that.fieldNameConverterClass != null)
            {
                return false;
            }
            if (indexNameConverterClass != null ? !indexNameConverterClass.equals(that.indexNameConverterClass) : that.indexNameConverterClass != null)
            {
                return false;
            }
            if (sequenceNameConverterClass != null ? !sequenceNameConverterClass.equals(that.sequenceNameConverterClass) : that.sequenceNameConverterClass != null)
            {
                return false;
            }
            if (tableNameConverterClass != null ? !tableNameConverterClass.equals(that.tableNameConverterClass) : that.tableNameConverterClass != null)
            {
                return false;
            }
            if (triggerNameConverterClass != null ? !triggerNameConverterClass.equals(that.triggerNameConverterClass) : that.triggerNameConverterClass != null)
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = databaseUpdaterClass != null ? databaseUpdaterClass.hashCode() : 0;
            result = 31 * result + (tableNameConverterClass != null ? tableNameConverterClass.hashCode() : 0);
            result = 31 * result + (fieldNameConverterClass != null ? fieldNameConverterClass.hashCode() : 0);
            result = 31 * result + (sequenceNameConverterClass != null ? sequenceNameConverterClass.hashCode() : 0);
            result = 31 * result + (triggerNameConverterClass != null ? triggerNameConverterClass.hashCode() : 0);
            result = 31 * result + (indexNameConverterClass != null ? indexNameConverterClass.hashCode() : 0);
            result = 31 * result + (withIndex ? 1 : 0);
            return result;
        }
    }

}
