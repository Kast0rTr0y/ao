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
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * URI data type specific tests
 */
@SuppressWarnings("unchecked")
public final class URITypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * URI should not support AutoIncrement
     */

    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * @NotNull not required for primary id column
     */
    @Test
    @NonTransactional
    public void testPrimaryWithoutNotNull() throws Exception
    {
        entityManager.migrate(PrimaryWithoutNotNull.class);
    }

    /**
     * Test creation using a URI id
     */
    @Test
    @NonTransactional
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        URI uri = new URI("http://www.google.com");
        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", uri));
        assertEquals(uri, e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", "http://www.google.com");
    }

    /**
     * Null can't be used as id
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Test different values for a URI column (ID column in this case)
     */
    @Test
    @NonTransactional
    public void testColumnValues() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        for (URI uri : new URI[] {new URI("http://www.google.com"), new URI("http://localhost:2990/jira#anchor"), new URI("file://localhost/etc/passwd"), new URI("https://google.com?q=active%20objects"), new URI("../../../etc/passwd"), new URI("file:///~/.m2")})
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", uri));
            entityManager.flushAll();
            assertEquals(uri, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", uri.toString());
        }
    }

    /**
     * Update a simple uri column
     */
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getUri());

        // set
        URI uri = new URI("http://google.com");
        e.setUri(uri);
        e.save();
        entityManager.flushAll();

        assertEquals(uri, e.getUri());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUri", uri.toString());
    }

    /**
     * Empty string is treated as null on certain databases, disallow
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);

        entityManager.create(EmptyDefaultColumn.class);
    }

    /**
     * Invalid default values
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);

        entityManager.create(InvalidDefaultColumn.class);
    }

    /**
     * Check different default values
     */
    @Test
    @NonTransactional
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(new URI("http://www.google.com?q=active%20objects"), e.getUri());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getUri", "http://www.google.com?q=active%20objects");
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithCreate() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getUri"), null));

        entityManager.flushAll();
        assertNull(e.getUri());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUri", null);
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithSet() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getUri"), new URI("http://localhost:2990/jira#anchor")));
        e.setUri(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getUri());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getUri", null);
    }

    @Test
    @NonTransactional
    public void testNullValueWithPullFromDatabase() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn newEntity = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getUri"), 
                new URI("http://localhost:2990/jira#anchor")));
        newEntity.setUri(null);
        newEntity.save();

        entityManager.flushAll();

        //Use PullFromDatabase of URIType
        SimpleColumn loadedEntity = entityManager.get(SimpleColumn.class, newEntity.getID());
        assertNull(loadedEntity.getUri());
    }

    /**
     * Test a not null column constraint column
     */
    @Test
    @NonTransactional
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        URI uri = new URI("http://www.google.com?q=active%20objects");
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUri"), uri));

        entityManager.flushAll();
        assertEquals(uri, e.getUri());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getUri", "http://www.google.com?q=active%20objects");
    }

    /**
     * Not providing a value for a not null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
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
    @NonTransactional
    public void testNotNullColumnNullValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUri"), null));
    }

    /**
     * create with a string instead of a URI object
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testWrongDatatype() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUri"), Boolean.FALSE));
    }

    /**
     * create with a string instead of a URI object, yet the string represents a valid URI
     * this should not be allowed, since it'd potentially allow writing of data that can't be read
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testWrongDatatypeCorrectData() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUri"), "http://www.google.com"));
    }

    /**
     * create with a string instead of a URI object, yet the string represents an invalid URI
     * this should not be allowed, since it'd allow writing of data that can't be read
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testWrongDatatypeWrongData() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        entityManager.create(SimpleColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getUri"), "blah://www.google.com"));
    }


    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String isGetterName, final Object id, final String getterName, final String fieldValue) throws Exception
    {
        executeStatement("SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, isGetterName) + " = ?",
            new DbUtils.StatementCallback()
            {
                public void setParameters(PreparedStatement statement) throws Exception
                {
                    if (id instanceof URI)
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
    public static interface AutoIncrementId extends RawEntity<URI>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        @StringLength(255)
        public URI getId();
    }

    /**
     * Primary column without NotNull annotation - should be ok
     */
    public static interface PrimaryWithoutNotNull extends RawEntity<URI>
    {
        @PrimaryKey("ID")
        @StringLength(255)
        public URI getId();
    }

    /**
     * Simple primary column
     */
    public static interface SimpleId extends RawEntity<URI>
    {
        @NotNull
        @PrimaryKey("ID")
        @StringLength(255)
        public URI getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }

    /**
     * Empty URI for default value - not supported
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }

    /**
     * Default values
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("http://www.google.com?q=active%20objects")
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }

    public static interface InvalidDefaultColumn extends Entity
    {
        @Default(":\\NULL*")
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }

    /**
     * Indexed column - not supported
     */
    public static interface Indexed extends Entity
    {
        @net.java.ao.schema.Indexed
        @StringLength(255)
        public URI getUri();
        public void setUri(URI uri);
    }
}
