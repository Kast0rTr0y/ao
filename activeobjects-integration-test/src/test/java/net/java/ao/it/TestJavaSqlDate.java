package net.java.ao.it;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.Entity;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

public final class TestJavaSqlDate extends ActiveObjectsIntegrationTest
{
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testJavaSqlDateIsNotSupported() throws Exception
    {
        entityManager.migrate(EntityWithJavaSqlDate.class);
    }

    public static interface EntityWithJavaSqlDate extends Entity
    {
        java.sql.Date getJavaSqlDate();
        void setJavaSqlDate(java.sql.Date time);
    }
}
