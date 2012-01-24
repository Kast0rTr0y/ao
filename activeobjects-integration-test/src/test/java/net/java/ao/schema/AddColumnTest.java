package net.java.ao.schema;

import net.java.ao.Entity;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.*;

public final class AddColumnTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testAddColumn() throws Exception
    {
        entityManager.migrate(Clean.T.class);

        final Clean.T t = entityManager.create(Clean.T.class);

        checkSqlExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                entityManager.migrate(AddedColumn.T.class);
                return null;
            }
        });

        entityManager.flushAll();

        final AddedColumn.T newT = entityManager.get(AddedColumn.T.class, t.getID());
        assertNotNull(newT);
        assertNull(newT.getColumn());

        newT.setColumn("some value");
        newT.save();
    }

    static final class Clean
    {
        public static interface T extends Entity
        {
        }
    }

    static final class AddedColumn
    {
        public static interface T extends Entity
        {
            String getColumn();

            void setColumn(String column);
        }
    }
}
