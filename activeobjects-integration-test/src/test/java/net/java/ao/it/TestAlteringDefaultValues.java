package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.Default;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.converters.NameConverters;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

@NameConverters(table = TestTableNameConverter.class)
public final class TestAlteringDefaultValues extends ActiveObjectsIntegrationTest {
    private static final String DEFAULT_1 = "default_1";

    @Test
    public void testDefaultMigrated() throws Exception {
        entityManager.migrate(EntityVersion1.class);

        checkSqlNotExecuted(new Callable<Object>() {
            @Override
            public Void call() throws Exception {
                entityManager.migrate(EntityVersion1.class);
                return null;
            }
        });


        final EntityVersion1 v1 = entityManager.create(EntityVersion1.class);
        assertEquals(DEFAULT_1, v1.getDescription());

        checkSqlExecuted(new Callable<Object>() {
            @Override
            public Void call() throws Exception {
                entityManager.migrate(EntityVersion2.class);
                return null;
            }
        });

        final EntityVersion2 v2 = entityManager.create(EntityVersion2.class);
        assertEquals(null, v2.getDescription());

        checkSqlNotExecuted(new Callable<Object>() {
            @Override
            public Void call() throws Exception {
                entityManager.migrate(EntityVersion2.class);
                return null;
            }
        });

        final EntityVersion2 v2_2 = entityManager.create(EntityVersion2.class);
        assertEquals(null, v2_2.getDescription());
    }

    @Test
    public void testNotMigratedTwiceWithDefault() throws Exception {
        testEntityNotMigratedTwice(EntityVersion1.class);
    }

    @Test
    public void testNotMigratedTwiceWithNoDefault() throws Exception {
        testEntityNotMigratedTwice(EntityVersion2.class);
    }

    private void testEntityNotMigratedTwice(final Class<? extends RawEntity<?>> entityClass) throws Exception {
        entityManager.migrate(entityClass);

        checkSqlNotExecuted(new Callable<Object>() {
            @Override
            public Void call() throws Exception {
                entityManager.migrate(entityClass);
                return null;
            }
        });
    }

    static interface TestEntity extends Entity {
        String getDescription();
    }

    static interface EntityVersion1 extends TestEntity {
        @Default(DEFAULT_1)
        String getDescription();

        void setDescription(String desc);
    }

    static interface EntityVersion2 extends TestEntity {
        String getDescription();

        void setDescription(String desc);
    }

    public static class TestTableNameConverter implements TableNameConverter {
        @Override
        public String getName(Class<? extends RawEntity<?>> clazz) {
            return "TEST_ENTITY";
        }
    }
}
