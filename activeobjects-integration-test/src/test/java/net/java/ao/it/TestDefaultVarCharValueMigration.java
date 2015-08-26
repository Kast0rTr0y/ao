package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.schema.Default;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.util.concurrent.Callable;

@Data(TestDefaultVarCharValueMigration.TestDefaultVarCharValueMigrationUpdater.class)
public final class TestDefaultVarCharValueMigration extends ActiveObjectsIntegrationTest {
    @Test
    public void doSomething() throws Exception {
        checkSqlNotExecuted(new Callable<Object>() {
            @Override
            public Void call() throws Exception {
                entityManager.migrate(TestEntity.class);
                return null;
            }
        });
    }

    static interface TestEntity extends Entity {
        @Default("default")
        String getKey();

        void setKey(String key);
    }

    public static final class TestDefaultVarCharValueMigrationUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestEntity.class);
        }
    }
}
