package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Callable;

@Data(TestMySqlAlterWithTimestamp.TestMySqlAlterWithTimestampUpdater.class)
public final class TestMySqlAlterWithTimestamp extends ActiveObjectsIntegrationTest {
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

    private void newTestEntity() throws SQLException {
        final TestEntity testEntity = entityManager.create(TestEntity.class);
        testEntity.setCreated(new Date());
        testEntity.save();
    }

    static interface TestEntity extends Entity {
        void setCreated(Date created);

        Date getCreated();

        void setUpdated(Date updated);

        Date getUpdated();
    }

    public static final class TestMySqlAlterWithTimestampUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestEntity.class);
        }
    }
}
