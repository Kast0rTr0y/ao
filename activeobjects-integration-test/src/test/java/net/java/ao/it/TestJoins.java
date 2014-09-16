package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.OneToMany;
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

@Data (TestJoins.TestJoinsDatabaseUpdater.class)
public class TestJoins extends ActiveObjectsIntegrationTest
{
    @Test
    public void joinRetrievesAllColumns() throws Exception
    {
        final Query query = Query.select()
                .alias(From.class, "f")
                .alias(To.class, "t")
                .join(To.class, "f.TO_ID = t.ID")
                .where("f.VALUE = ?", FromData.VALUES[0])
                .limit(1);

        final From[] froms = checkSqlExecuted(new Callable<From[]>()
        {
            @Override
            public From[] call() throws Exception
            {
                return entityManager.find(From.class, query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                froms[0].getValue();
                return null;
            }
        });
    }

    @Test
        public void joinPreloadRetrievesAllColumns() throws Exception
    {
        final Query query = Query.select()
                .alias(FromPreload.class, "f")
                .alias(To.class, "t")
                .join(To.class, "f.TO_ID = t.ID")
                .where("f.VALUE = ?", FromData.VALUES[0])
                .limit(1);

        final FromPreload[] froms = checkSqlExecuted(new Callable<FromPreload[]>()
        {
            @Override
            public FromPreload[] call() throws Exception
            {
                return entityManager.find(FromPreload.class, query);
            }
        });

        assertNotNull(froms);
        assertTrue(froms.length == 1);

        checkSqlNotExecuted(new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                froms[0].getValue();
                return null;
            }
        });
    }

    public static final class FromData
    {
        static final String[] VALUES = { "value1", "value2" };
    }

    static interface From extends Entity
    {
        void setValue(String value);

        String getValue();

        void setTo(To to);

        To getTo();
    }

    @Preload
    static interface FromPreload extends Entity
    {
        void setValue(String value);

        String getValue();

        void setTo(To to);

        To getTo();
    }

    static final class ToData
    {
        static final String[] VALUES = { "value1", "value2" };
    }

    static interface To extends Entity
    {
        void setValue(String value);

        String getValue();

        @OneToMany
        From[] getFroms();

        @OneToMany
        From[] getFromPreloads();
    }

    public static final class TestJoinsDatabaseUpdater implements DatabaseUpdater
    {
        public void update(EntityManager entityManager) throws Exception
        {
            //noinspection unchecked
            entityManager.migrate(From.class, FromPreload.class, To.class);

            final List<To> tos = new ArrayList<To>();
            for (final String value : ToData.VALUES)
            {
                tos.add(entityManager.create(To.class,  new DBParam("VALUE", value)));
            }

            for (final String value : FromData.VALUES)
            {
                entityManager.create(From.class, new DBParam("VALUE", value), new DBParam("TO_ID", tos.get(0)));
                entityManager.create(FromPreload.class, new DBParam("VALUE", value), new DBParam("TO_ID", tos.get(0)));
            }
        }
    }
}
