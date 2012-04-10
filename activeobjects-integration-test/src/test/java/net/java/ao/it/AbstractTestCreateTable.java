package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

@NameConverters(table = AbstractTestCreateTable.TestCreateTableTableNameConverter.class)
public abstract class AbstractTestCreateTable extends ActiveObjectsIntegrationTest
{
    @Test
    @NonTransactional
    public final void testCreateTable() throws Exception
    {
        for (Class<? extends Entity> entity : getEntities())
        {
            testCreateTable(entity);
        }
    }

    protected abstract List<Class<? extends Entity>> getEntities();

    private void testCreateTable(final Class<? extends Entity> entityClass) throws Exception
    {
        checkSqlExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                entityManager.migrate(entityClass);
                return null;
            }
        });

        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                entityManager.migrate(entityClass);
                return null;
            }
        });
    }

    public static final class TestCreateTableTableNameConverter implements TableNameConverter
    {
        @Override
        public String getName(Class<? extends RawEntity<?>> clazz)
        {
            return "ENTITY";
        }
    }
}