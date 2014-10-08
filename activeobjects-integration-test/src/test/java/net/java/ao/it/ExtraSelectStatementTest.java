package net.java.ao.it;

import java.net.URI;
import java.net.URL;
import java.util.Date;
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
        // All supported databases and java type = URL
        final Lego lego = entityManager.find(Lego.class)[0];

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                lego.getClobType();
                lego.isBooleanType();
                lego.getURLType();
                lego.getStringType();
                lego.getBlobType();
                lego.getURIType();
                lego.getDateType();
                lego.getDoubleType();
                lego.getEnumType();
                lego.getIntegerType();
                lego.getLongType();

                return null;
            }
        });
    }

    private interface Lego extends Entity
    {
        static final String CLOB_CONTENT = "A Clob type for Oracle and Microsoft SQL Server instead of String";
        static final String STRING_CONTENT = "Lego name";
        static final String BLOB_CONTENT = "Image content";

        //ClobType
        @StringLength(StringLength.UNLIMITED)
        String getClobType();

        @StringLength(StringLength.UNLIMITED)
        void setClobType(String clobContent);

        //URLType
        @Accessor("url")
        public URL getURLType();

        @Default("http://www.abc.com")
        @Mutator("url")
        public void setURLType(URL url);

        //URIType
        @Accessor("uri")
        public URI getURIType();

        @Default("ftp://abc.org/resource.txt")
        @Mutator("uri")
        public void setURIType(URI uri);

        //StringType
        String getStringType();

        void setStringType(String stringType);

        //BooleanType
        boolean isBooleanType();

        void setBooleanType(boolean booleanType);

        //BlobType
        byte[] getBlobType();

        void setBlobType(byte[] blobContent);

        //DateType
        @Default("2014-10-08 00:00:00")
        Date getDateType();

        void setDateType(Date dateType);

        //DoubleType
        Double getDoubleType();

        void setDoubleType(Double doubleType);

        //EnumType
        Level getEnumType();

        void setEnumType(Level enumType);

        //IntegerType
        Integer getIntegerType();

        void setIntegerType(Integer integerType);

        //LongType
        Long getLongType();

        void setLongType(Long longType);
    }

    public enum Level
    {
        HIGH,
        MEDIUM,
        LOW
    }

    public static final class TestExtraSelectStatementDatabaseUpdater implements DatabaseUpdater
    {
        @Override
        public void update(final EntityManager entityManager) throws Exception
        {
            entityManager.migrate(Lego.class);

            final Lego e = entityManager.create(Lego.class);
            e.setClobType(Lego.CLOB_CONTENT);
            e.setBooleanType(true);
            e.setStringType(Lego.STRING_CONTENT);
            e.setBlobType(Lego.BLOB_CONTENT.getBytes());
            e.setEnumType(Level.HIGH);
            e.setIntegerType(Integer.valueOf("1"));
            e.setDoubleType(Double.valueOf("1"));
            e.setLongType(Long.valueOf("1"));

            e.save();
        }
    }
}
