package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

/**
 * test to reproduce https://ecosystem.atlassian.net/browse/AO-699
 */
@Data(TestSetStringNull.EntityDatabaseUpdater.class)
public class TestSetStringNull extends ActiveObjectsIntegrationTest {

    @Test
    public void setStringNull() throws SQLException {
        TestEntity testEntity = entityManager.create(TestEntity.class);
        testEntity.setComplete(true);
        testEntity.setDescription(null);
        testEntity.save();
    }

    public static class EntityDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestEntity.class);
        }
    }

    public interface TestEntity extends Entity {
        String getDescription();
        void setDescription(String description);
        boolean isComplete();
        void setComplete(boolean complete);
    }
}
