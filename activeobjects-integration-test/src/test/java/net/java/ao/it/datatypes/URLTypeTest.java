package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.Default;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.StringLength;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import org.junit.Test;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * URL data type specific tests
 */
@SuppressWarnings("unchecked")
public final class URLTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * URL should not support AutoIncrement
     */

    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * @NotNull not required for primary id column
     */
    @Test
    public void testPrimaryWithoutNotNull() throws Exception
    {
        entityManager.migrate(PrimaryWithoutNotNull.class);
    }

    /**
     * Test creation using a URL id
     */
    @Test
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        URL url = new URL("http://www.google.com");
        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", url));
        assertEquals(url, e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", "http://www.google.com");
    }

    /**
     * Null can't be used as id
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Test different values for a URL column (ID column in this case)
     */
    @Test
    public void testColumnValues() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        for (URL url : new URL[] {new URL("http://www.google.com"), new URL("http://localhost:2990/jira#anchor"), new URL("file://localhost/etc/passwd"), new URL("https://google.com?q=active objects")})
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", url));
            entityManager.flushAll();
            assertEquals(url, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", url.toString());
        }
    }

    /**
     * Update a simple url column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getUrl());

        // set
        URL url = new URL("http://google.com");
        e.setUrl(url);
        e.save();
        entityManager.flushAll();

        assertEquals(url, e.getUrl());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUrl", url.toString());
    }

    /**
     * Empty string is treated as null on certain databases, disallow
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);

        entityManager.create(EmptyDefaultColumn.class);
    }

    /**
     * Invalid default values
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);

        entityManager.create(InvalidDefaultColumn.class);
    }

    /**
     * Check different default values
     */
    @Test
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(new URL("http://www.google.com?q=active%20objects"), e.getUrl());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getUrl", "http://www.google.com?q=active%20objects");
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithCreate() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getUrl"), null));

        entityManager.flushAll();
        assertNull(e.getUrl());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUrl", null);
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithSet() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getUrl"), new URL("http://localhost:2990/jira#anchor")));
        e.setUrl(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getUrl());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUrl", null);
    }

    /**
     * Test a not null column constraint column
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        URL url = new URL("http://www.google.com?q=active%20objects");
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUrl"), url));

        entityManager.flushAll();
        assertEquals(url, e.getUrl());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getUrl", "http://www.google.com?q=active%20objects");
    }

    /**
     * Not providing a value for a not null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNoValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class);
    }

    /**
     * Inserting null in a not null column should throw an Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNullValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUrl"), null));
    }

    /**
     * create with a string instead of a URL object
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongDatatype() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUrl"), Boolean.FALSE));

        e.getUrl();
    }

    /**
     * create with a string instead of a URL object, yet the string represents a valid URL
     * this should not be allowed, since it'd potentially allow writing of data that can't be read
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongDatatypeCorrectData() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUrl"), "http://www.google.com"));

        e.getUrl();
    }

    /**
     * create with a string instead of a URL object, yet the string represents an invalid URL
     * this should not be allowed, since it'd allow writing of data that can't be read
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWrongDatatypeWrongData() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUrl"), "blah://www.google.com"));

        e.getUrl();
    }


    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final String fieldValue) throws Exception
    {
        executeStatement("SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
            new DbUtils.StatementCallback()
            {
                public void setParameters(PreparedStatement statement) throws Exception
                {
                    if (id instanceof URL)
                    {
                        statement.setString(1, id.toString());
                    }
                    else
                    {
                        statement.setObject(1, id);
                    }
                }

                public void processResult(ResultSet resultSet) throws Exception
                {
                    if (resultSet.next())
                    {
                        assertEquals(fieldValue, resultSet.getString(getFieldName(entityType, getterName)));
                    }
                    else
                    {
                        fail("No entry found in database with ID " + id);
                    }
                }
            }
        );
    }

    /**
     * Auto increment primary key column - not supported
     */
    public static interface AutoIncrementId extends RawEntity<URL>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        @StringLength(255)
        public URL getId();
    }

    /**
     * Primary column without NotNull annotation - should be ok
     */
    public static interface PrimaryWithoutNotNull extends RawEntity<URL>
    {
        @PrimaryKey("ID")
        @StringLength(255)
        public URL getId();
    }

    /**
     * Simple primary column
     */
    public static interface SimpleId extends RawEntity<URL>
    {
        @NotNull
        @PrimaryKey("ID")
        @StringLength(255)
        public URL getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }

    /**
     * Empty URL for default value - not supported
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }

    /**
     * Default values
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("http://www.google.com?q=active%20objects")
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }

    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("NULL")
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }

    /**
     * Indexed column - not supported
     */
    public static interface Indexed extends Entity
    {
        @net.java.ao.schema.Indexed
        @StringLength(255)
        public URL getUrl();
        public void setUrl(URL url);
    }
}
