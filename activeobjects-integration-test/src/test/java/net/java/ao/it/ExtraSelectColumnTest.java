package net.java.ao.it;

import java.net.URL;
import java.util.concurrent.Callable;

import org.junit.Test;

import net.java.ao.Accessor;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Mutator;
import net.java.ao.Query;
import net.java.ao.schema.Default;
import net.java.ao.schema.StringLength;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;

@Data(ExtraSelectColumnTest.TestExtraSelectStatementDatabaseUpdater.class)
public class ExtraSelectColumnTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testExtraSelectColumn() throws Exception
    {
        final Lego lego = entityManager.find(Lego.class, Query
                .select(getFieldName(Lego.class, "getID") + ", "
                        + getFieldName(Lego.class, "getDescription") + ", "
                        + getFieldName(Lego.class, "isExpired")))[0];

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                lego.getDescription();
                lego.isExpired();

                return null;
            }
        });

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                lego.getURL();

                return null;
            }
        });

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                lego.getName();

                return null;
            }
        });
    }

    private interface Lego extends Entity
    {
        static final String DESCRIPTION = "A Clob type for Oracle and Microsoft SQL Server instead of String";
        static final String NAME = "Lego name";

        @StringLength(StringLength.UNLIMITED)
        String getDescription();

        @StringLength(StringLength.UNLIMITED)
        void setDescription(String description);

        @Accessor("url")
        public URL getURL();

        @Default("http://www.google.com")
        @Mutator("url")
        @StringLength(255)
        public void setURL(URL url);

        String getName();

        void setName(String name);

        boolean isExpired();

        void setExpired(boolean expired);
    }

    public static final class TestExtraSelectStatementDatabaseUpdater implements DatabaseUpdater
    {
        @Override
        public void update(final EntityManager entityManager) throws Exception
        {
            entityManager.migrate(Lego.class);

            final Lego e = entityManager.create(Lego.class);
            e.setDescription(Lego.DESCRIPTION);
            e.setExpired(true);
            e.setName(Lego.NAME);

            e.save();
        }
    }
}
