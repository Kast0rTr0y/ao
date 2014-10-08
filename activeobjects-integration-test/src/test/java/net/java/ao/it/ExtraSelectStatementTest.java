package net.java.ao.it;

import java.net.URL;
import java.util.concurrent.Callable;

import org.junit.Test;

import net.java.ao.Accessor;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Mutator;
import net.java.ao.schema.Default;
import net.java.ao.schema.StringLength;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;

@Data(ExtraSelectStatementTest.TestExtraSelectStatementDatabaseUpdater.class)
public class ExtraSelectStatementTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testExtraSelectWhenClobTypeInOracleAndSQLServer() throws Exception
    {
        // Test AO runs extra select statement when :
        // Oracle database and type = Boolean, Clob
        // MS SQL Server and type = Clob
        // Oracle & MS SQL Server and java type = URL
        final Lego lego = entityManager.find(Lego.class)[0];

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                lego.getDescription();
                lego.isExpired();
                lego.getURL();
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

        @Default("http://www.abc.com")
        @Mutator("url")
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
