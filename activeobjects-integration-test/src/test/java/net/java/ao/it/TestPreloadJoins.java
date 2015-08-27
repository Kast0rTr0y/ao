package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Preload;
import net.java.ao.Query;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Data(TestPreloadJoins.TestPreloadJoinsDatabaseUpdater.class)
public class TestPreloadJoins extends ActiveObjectsIntegrationTest {
    @Test
    public void joinRetrievesAllValues() throws Exception {
        final Query query = Query.select()
                .alias(From.class, "f")
                .alias(To.class, "t")
                .join(To.class, "f.TO_ID = t.ID")
                .where("f.VALUE = ?", FromData.VALUES[0])
                .limit(1);

        final From[] froms = checkSqlExecuted(new Callable<From[]>() {
            @Override
            public From[] call() throws Exception {
                return entityManager.find(From.class, query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getValue();
                return null;
            }
        });
    }

    @Test
    public void joinPreloadAllRetrievesAllValues() throws Exception {
        final Query query = Query.select()
                .alias(FromPreloadAll.class, "f")
                .alias(To.class, "t")
                .join(To.class, "f.TO_ID = t.ID")
                .where("f.VALUE = ?", FromData.VALUES[0])
                .limit(1);

        final FromPreloadAll[] froms = checkSqlExecuted(new Callable<FromPreloadAll[]>() {
            @Override
            public FromPreloadAll[] call() throws Exception {
                return entityManager.find(FromPreloadAll.class, query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getValue();
                return null;
            }
        });
    }

    @Test
    public void joinPreloadSomeRetrievesSpecifiedAndPreloadValues() throws Exception {
        final Query query = Query.select("OTHER_VALUE, ID")
                .alias(FromPreloadSome.class, "f")
                .alias(To.class, "t")
                .join(To.class, "f.TO_ID = t.ID")
                .where("f.VALUE = ?", FromData.VALUES[0])
                .limit(1);

        final FromPreloadSome[] froms = checkSqlExecuted(new Callable<FromPreloadSome[]>() {
            @Override
            public FromPreloadSome[] call() throws Exception {
                return entityManager.find(FromPreloadSome.class, "ID", query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getValue();
                froms[0].getOtherValue();
                return null;
            }
        });

        checkSqlExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getYetAnotherValue();
                return null;
            }
        });
    }

    @Test
    public void joinWithoutAlias() throws Exception {
        final Query query = Query.select("OTHER_VALUE, ID")
                .from(FromPreloadSome.class)
                .alias(To.class, "t")
                .join(To.class, "TO_ID = t.ID")
                .where("YET_ANOTHER_VALUE = ?", "yav")
                .limit(1);

        final FromPreloadSome[] froms = checkSqlExecuted(new Callable<FromPreloadSome[]>() {
            @Override
            public FromPreloadSome[] call() throws Exception {
                return entityManager.find(FromPreloadSome.class, "ID", query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getValue();
                froms[0].getOtherValue();
                return null;
            }
        });

        checkSqlExecuted(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                froms[0].getYetAnotherValue();
                return null;
            }
        });
    }

    public static final class FromData {
        static final String[] VALUES = {"value1", "value2"};
    }

    static interface From extends Entity {
        void setValue(String value);

        String getValue();

        void setTo(To to);

        To getTo();
    }

    @Preload
    static interface FromPreloadAll extends Entity {
        void setValue(String value);

        String getValue();

        void setTo(To to);

        To getTo();
    }

    @Preload("VALUE")
    static interface FromPreloadSome extends Entity {
        void setValue(String value);

        String getValue();

        void setOtherValue(String otherValue);

        String getOtherValue();

        void setYetAnotherValue(String yetAnotherValue);

        String getYetAnotherValue();

        void setTo(To to);

        To getTo();
    }

    static final class ToData {
        static final String[] VALUES = {"value1", "value2"};
    }

    static interface To extends Entity {
        void setValue(String value);

        String getValue();
    }

    public static final class TestPreloadJoinsDatabaseUpdater implements DatabaseUpdater {
        public void update(EntityManager entityManager) throws Exception {
            //noinspection unchecked
            entityManager.migrate(From.class, FromPreloadAll.class, FromPreloadSome.class, To.class);

            final List<To> tos = new ArrayList<To>();
            for (final String value : ToData.VALUES) {
                tos.add(entityManager.create(To.class, new DBParam("VALUE", value)));
            }

            for (final String value : FromData.VALUES) {
                entityManager.create(From.class, new DBParam("VALUE", value), new DBParam("TO_ID", tos.get(0)));
                entityManager.create(FromPreloadAll.class, new DBParam("VALUE", value), new DBParam("TO_ID", tos.get(0)));
                entityManager.create(FromPreloadSome.class, new DBParam("VALUE", value), new DBParam("YET_ANOTHER_VALUE", "yav"), new DBParam("OTHER_VALUE", "ov"), new DBParam("TO_ID", tos.get(0)));
            }
        }
    }
}
