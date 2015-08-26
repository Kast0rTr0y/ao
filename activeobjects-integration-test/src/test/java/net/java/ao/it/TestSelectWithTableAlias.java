package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Preload;
import net.java.ao.Query;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.util.concurrent.Callable;

import static net.java.ao.it.TestSelectWithTableAlias.TestSelectWithTableAliasUpdater.DESCRIPTION;
import static net.java.ao.it.TestSelectWithTableAlias.TestSelectWithTableAliasUpdater.NAME;
import static org.junit.Assert.assertEquals;

@Data(TestSelectWithTableAlias.TestSelectWithTableAliasUpdater.class)
public final class TestSelectWithTableAlias extends ActiveObjectsIntegrationTest {
    @Test
    public void testSimpleQuery() throws Exception {
        final TestEntity[] es = entityManager.find(TestEntity.class, Query.select().alias(TestEntity.class, "te"));
        assertEquals(1, es.length);
        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(DESCRIPTION, es[0].getDescription());
                return null;
            }
        });
    }

    @Test
    public void testQueryWithJoin() throws Exception {
        final String eIdColumn = getFieldName(TestEntity.class, "getID");
        final String eDescriptionColumn = getFieldName(TestEntity.class, "getDescription");
        final String oIdColumn = getFieldName(TestOtherEntity.class, "getID");
        final String oNameColumn = getFieldName(TestOtherEntity.class, "getName");

        final TestEntity[] es = entityManager.find(TestEntity.class, Query.select(eIdColumn + "," + eDescriptionColumn)
                .alias(TestEntity.class, "e").alias(TestOtherEntity.class, "o")
                .join(TestOtherEntity.class, "e." + eIdColumn + " = o." + oIdColumn)
                .where("o." + oNameColumn + " = ?", NAME));

        assertEquals(1, es.length);
        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                assertEquals(DESCRIPTION, es[0].getDescription());
                return null;
            }
        });
    }

    @Preload("description")
    public static interface TestEntity extends Entity {
        String getDescription();

        void setDescription(String description);

        TestOtherEntity getOtherEntity();

        void setOtherEntity(TestOtherEntity e);
    }

    public static interface TestOtherEntity extends Entity {
        String getName();

        void setName(String name);
    }

    public static final class TestSelectWithTableAliasUpdater implements DatabaseUpdater {
        static final String NAME = "som-e-n-am--e";
        static final String DESCRIPTION = "bla";

        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestEntity.class, TestOtherEntity.class);

            final TestOtherEntity o = entityManager.create(TestOtherEntity.class);
            o.setName(NAME);
            o.save();

            final TestEntity e = entityManager.create(TestEntity.class);
            e.setDescription(DESCRIPTION);
            e.setOtherEntity(o);
            e.save();
        }
    }
}
