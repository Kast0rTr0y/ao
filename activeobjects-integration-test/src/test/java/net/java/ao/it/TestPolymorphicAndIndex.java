package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.Polymorphic;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.NotNull;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Test;

/**
 * Test for ACTIVEOBJECTS-64 where {@link Indexed} fields within {@link Polymorphic} entities try to create extra index
 * on non-existing tables.
 */
public final class TestPolymorphicAndIndex extends ActiveObjectsIntegrationTest {
    @Test
    public void createDatabaseSchema() throws Exception {
        entityManager.migrate(ReferenceableEntity.class, Achievement.class);
        entityManager.create(Achievement.class, new DBParam(getFieldName(Achievement.class, "getRef"), "some-ref")).save();
    }


    @Polymorphic
    static interface ReferenceableEntity extends Entity {
        @Indexed
        @NotNull
        public String getRef();

        public void setRef(String ref);
    }

    static interface Achievement extends ReferenceableEntity {
    }
}
