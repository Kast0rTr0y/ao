package net.java.ao.it.datatypes;

import net.java.ao.Entity;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import static org.junit.Assert.*;

public final class MigrationFromIntegerToLongTest extends ActiveObjectsIntegrationTest
{
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception
    {
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


    @Table("ENTITY")
    public static interface SimpleIntegerColumn extends Entity
    {
        public Integer getAge();
        public void setAge(Integer age);
    }

    @Table("ENTITY")
    public static interface SimpleLongColumn extends Entity
    {
        public Long getAge();
        public void setAge(Long age);
    }
}
