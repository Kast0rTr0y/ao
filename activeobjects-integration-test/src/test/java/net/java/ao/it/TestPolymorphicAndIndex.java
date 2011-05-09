package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Polymorphic;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.Unique;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.test.jdbc.Jdbc;
import org.junit.Test;

/**
 * Test for ACTIVEOBJECTS-64 where {@link Indexed} fields within {@link Polymorphic} entities try to create extra index
 * on non-existing tables.
 */
@Jdbc(DynamicJdbcConfiguration.class)
public class TestPolymorphicAndIndex extends ActiveObjectsIntegrationTest
{
    private EntityManager entityManager;

    @Test
//    @Ignore
    public void createDatabaseSchema() throws Exception
    {
        entityManager.migrate(ReferenceableEntity.class, Achievement.class);
        entityManager.create(Achievement.class, new DBParam("ref", "some-ref")).save();
    }


    @Polymorphic
    static interface ReferenceableEntity extends Entity
    {
        @Indexed
        @NotNull
        @Unique
        public String getRef();

        public void setRef(String ref);
    }

    static interface Achievement extends ReferenceableEntity
    {
    }
}
