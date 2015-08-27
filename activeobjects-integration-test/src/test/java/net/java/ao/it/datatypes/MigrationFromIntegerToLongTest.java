package net.java.ao.it.datatypes;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class MigrationFromIntegerToLongTest extends ActiveObjectsIntegrationTest {
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception {
        entityManager.migrate(SimpleIntegerColumn.class);

        final SimpleIntegerColumn e = entityManager.create(SimpleIntegerColumn.class);
        e.setAge(Integer.MAX_VALUE);
        e.save();

        entityManager.migrate(SimpleLongColumn.class);
        entityManager.flushAll();

        SimpleLongColumn retrieved = entityManager.get(SimpleLongColumn.class, e.getID());
        assertEquals(new Long(Integer.MAX_VALUE), retrieved.getAge());

        retrieved.setAge(Long.MAX_VALUE);
        retrieved.save();
    }

    @Test
    @NonTransactional
    public void testAutoIncrementId() throws Exception {
        final int age = 123;

        entityManager.migrate(SimpleIntegerColumn.class);

        final SimpleIntegerColumn e = entityManager.create(SimpleIntegerColumn.class);
        e.setAge(age);
        e.save();

        entityManager.migrate(AutoIncrementLong.class);
        entityManager.flushAll();

        AutoIncrementLong retrieved = entityManager.get(AutoIncrementLong.class, (long) e.getID());
        assertEquals(new Integer(age), retrieved.getAge());

        AutoIncrementLong newEntity = entityManager.create(AutoIncrementLong.class);
        assertTrue(newEntity.getID() != e.getID());
        assertEquals(2, entityManager.find(AutoIncrementLong.class).length);
    }


    @Table("ENTITY")
    public static interface SimpleIntegerColumn extends Entity {
        Integer getAge();

        void setAge(Integer age);
    }

    @Table("ENTITY")
    public static interface SimpleLongColumn extends Entity {
        Long getAge();

        void setAge(Long age);
    }

    @Table("ENTITY")
    public static interface AutoIncrementLong extends RawEntity<Long> {
        @PrimaryKey
        @AutoIncrement
        Long getID();

        Integer getAge();

        void setAge(Integer age);
    }
}
