package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.Unique;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.MySql;
import org.junit.Test;

import java.util.concurrent.Callable;

@NameConverters(table = TestMigrationOnUniqueColumnChange.TestTableNameConverter.class)
public final class TestMigrationOnUniqueColumnChange extends ActiveObjectsIntegrationTest
{
    @Test
    public void testUniqueAdded() throws Exception
    {
        entityManager.migrate(EntityVersion1.class);

        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Void call() throws Exception
            {
                entityManager.migrate(EntityVersion1.class);
                return null;
            }
        });

        checkSqlExecuted(new Callable<Object>()
        {
            @Override
            public Void call() throws Exception
            {
                entityManager.migrate(EntityVersion2.class);
                return null;
            }
        });
    }

    static interface EntityVersion1 extends Entity
    {
        String getKey();

        void setKey(String key);
    }

    static interface EntityVersion2 extends Entity
    {
        @Unique
        String getKey();

        void setKey(String key);
    }

    public static class TestTableNameConverter implements TableNameConverter
    {
        @Override
        public String getName(Class<? extends RawEntity<?>> clazz)
        {
            return "TEST_ENTITY";
        }
    }
}
