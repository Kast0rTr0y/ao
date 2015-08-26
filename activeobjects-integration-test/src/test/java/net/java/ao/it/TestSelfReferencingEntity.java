package net.java.ao.it;

import net.java.ao.DefaultPolymorphicTypeMapper;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Polymorphic;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Data(TestSelfReferencingEntity.SelfReferencingEntityDatabaseUpdater.class)
public final class TestSelfReferencingEntity extends ActiveObjectsIntegrationTest {
    @Test
    public void doTest() throws Exception {
        final Sub1SelfReferencing sub1 = entityManager.create(Sub1SelfReferencing.class);
        sub1.setName("sub1");
        sub1.setSub1("I am sub1");
        sub1.save();

        final Sub2SelfReferencing sub2 = entityManager.create(Sub2SelfReferencing.class);
        sub2.setName("sub2");
        sub2.setRef(sub1);
        sub2.setSub2("I am sub2");
        sub2.save();

        entityManager.flushAll();

        final SelfReferencing sub1Retrieved = entityManager.get(Sub1SelfReferencing.class, sub1.getID());
        assertEquals("sub1", sub1Retrieved.getName());
        assertEquals(null, sub1Retrieved.getRef());
        assertTrue(sub1Retrieved instanceof Sub1SelfReferencing);
        assertEquals("I am sub1", ((Sub1SelfReferencing) sub1Retrieved).getSub1());

        final SelfReferencing sub2Retrieved = entityManager.get(Sub2SelfReferencing.class, sub2.getID());
        assertEquals("sub2", sub2Retrieved.getName());
        assertEquals(sub1, sub2Retrieved.getRef());
        assertTrue(sub2Retrieved instanceof Sub2SelfReferencing);
        assertEquals("I am sub2", ((Sub2SelfReferencing) sub2Retrieved).getSub2());
    }

    public static final class SelfReferencingEntityDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.setPolymorphicTypeMapper(new DefaultPolymorphicTypeMapper(Sub1SelfReferencing.class, Sub2SelfReferencing.class));
            entityManager.migrate(Sub1SelfReferencing.class, Sub2SelfReferencing.class);
        }
    }

    @Polymorphic
    static interface SelfReferencing extends Entity {
        void setName(String name);

        String getName();

        void setRef(SelfReferencing ref);

        SelfReferencing getRef();
    }

    static interface Sub1SelfReferencing extends SelfReferencing {
        void setSub1(String sub);

        String getSub1();
    }

    static interface Sub2SelfReferencing extends SelfReferencing {
        void setSub2(String sub);

        String getSub2();
    }
}
