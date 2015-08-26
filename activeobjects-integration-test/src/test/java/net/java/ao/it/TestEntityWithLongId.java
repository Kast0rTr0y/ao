package net.java.ao.it;

import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Data(TestEntityWithLongId.TestEntityWithLongIdDatabaseUpdater.class)
public final class TestEntityWithLongId extends ActiveObjectsIntegrationTest {
    @Test
    public void testEntity() throws Exception {
        final EntityWithLongId e = entityManager.create(EntityWithLongId.class);

        assertNotNull(e.getId());
        assertTrue(e.getId() != 0L);
        assertNull(e.getDescription());

        e.setDescription("description");
        e.save();

        entityManager.flushAll();

        final EntityWithLongId e1 = entityManager.get(EntityWithLongId.class, e.getId());
        assertEquals(e.getId(), e1.getId());
        assertEquals(e.getDescription(), e1.getDescription());
    }

    public static class TestEntityWithLongIdDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(EntityWithLongId.class);
        }
    }

    public static interface EntityWithLongId extends RawEntity<Long> {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Long getId();

        String getDescription();

        void setDescription(String description);
    }
}
