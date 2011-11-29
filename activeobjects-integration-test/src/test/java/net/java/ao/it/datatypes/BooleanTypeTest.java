package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.Default;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.EntityUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public final class BooleanTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * Autoincrement does not make sense on Boolean fields, so AO should throw an exception
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * Test that a normal PK works
     */
    @Test
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", Boolean.TRUE));
        entityManager.flushAll();

        assertEquals(Boolean.TRUE, e.getId());
        checkFieldData(SimpleId.class, "getId", e.getId(), "getId", Boolean.TRUE);
    }

    /**
     * Inserting null into a PK field has to fail. Database level is fine here
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Boolean has a limited scope, so we can easily test all possible values
     */
    @Test
    public void testPossibleValues() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        // create a row with normal id
        for (Boolean data : new Boolean[] {Boolean.TRUE, Boolean.FALSE })
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", data));
            assertEquals(data, e.getId());
            checkFieldData(SimpleId.class, "getId", e.getId(), "getId", data);
        }
    }

    /**
     * Do a create and update on a regular boolean column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getData());

        // set
        e.setData(Boolean.TRUE);
        e.save();
        entityManager.flushAll();

        assertEquals(Boolean.TRUE, e.getData());
        checkFieldData(SimpleColumn.class, "getID", e.getID(), "getData", Boolean.TRUE);
    }

    /**
     * Test that delete with a boolean PK works
     */
    @Test
    public void testDelete() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        // create a record and assure it's in the DB
        SimpleId newRecord = entityManager.create(SimpleId.class, new DBParam("ID", Boolean.TRUE));
        entityManager.flushAll();
        checkFieldData(SimpleId.class, "getId", Boolean.TRUE, "getId", Boolean.TRUE);

        entityManager.delete(newRecord);
        entityManager.flushAll();

        executeStatement("SELECT * FROM " + EntityUtils.getTableName(entityManager, SimpleId.class), new DbUtils.StatementCallback()
        {

            @Override
            public void setParameters(PreparedStatement statement) throws Exception
            {
            }

            @Override
            public void processResult(ResultSet resultSet) throws Exception
            {
                assertFalse("table should have been empty", resultSet.next());
            }
        });
    }

    /**
     * A default empty string should not be allowed on a boolean type
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Test that correctly defined default values work (with different cases)
     */
    @Test
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);
        entityManager.flushAll();

        assertEquals(Boolean.TRUE, e.getDefaultTrue());
        assertEquals(Boolean.TRUE, e.getDefaultTrueCaps());
        assertEquals(Boolean.TRUE, e.getDefaultTrueMixed());
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultTrue", Boolean.TRUE);
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultTrueCaps", Boolean.TRUE);
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultTrueMixed", Boolean.TRUE);

        assertEquals(Boolean.FALSE, e.getDefaultFalse());
        assertEquals(Boolean.FALSE, e.getDefaultFalseCaps());
        assertEquals(Boolean.FALSE, e.getDefaultFalseMixed());
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultFalse", Boolean.FALSE);
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultFalseCaps", Boolean.FALSE);
        checkFieldData(DefaultColumn.class, "getID", e.getID(), "getDefaultFalseMixed", Boolean.FALSE);
    }

    /**
     * Column is set to NOT NULL, positive test
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getData"), Boolean.TRUE));

        entityManager.flushAll();
        assertEquals(Boolean.TRUE, e.getData());
        checkFieldData(NotNullColumn.class, "getID", e.getID(), "getData", Boolean.TRUE);
    }

    /**
     * Inserting null should not pass
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNullData() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getData"), null));
    }

    /**
     * Autoincrement doesn't make sense on boolean. This is to test that the framework correctly catches that.
     */
    public static interface AutoIncrementId extends RawEntity<Boolean>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Boolean getId();
    }

    /**
     * A good primary key
     */
    public static interface SimpleId extends RawEntity<Boolean>
    {
        @PrimaryKey("ID")
        public Boolean getId();
    }

    /**
     * A boolean column, straightforward
     */
    public static interface SimpleColumn extends Entity
    {
        public Boolean getData();
        public void setData(Boolean data);
    }

    /**
     * Since the default values are strings, testing what happens on empty strings
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Boolean getData();
        public void setData(Boolean data);
    }

    /**
     * Using a string that cannot be converted to a boolean
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Boolean getData();
        public void setData(Boolean data);
    }

    /**
     * using boolean values in different case combinations
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("true")
        public Boolean getDefaultTrue();
        public void setDefaultTrue(Boolean data);

        @Default("false")
        public Boolean getDefaultFalse();
        public void setDefaultFalse(Boolean data);

        @Default("TRUE")
        public Boolean getDefaultTrueCaps();
        public void setDefaultTrueCaps(Boolean data);

        @Default("FALSE")
        public Boolean getDefaultFalseCaps();
        public void setDefaultFalseCaps(Boolean data);

        @Default("True")
        public Boolean getDefaultTrueMixed();
        public void setDefaultTrueMixed(Boolean data);

        @Default("False")
        public Boolean getDefaultFalseMixed();
        public void setDefaultFalseMixed(Boolean data);
    }

    /**
     * with a constraint
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Boolean getData();
        public void setData(Boolean data);
    }

    private <T extends RawEntity<?>> void checkFieldData(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Boolean fieldData) throws Exception
    {
        executeStatement("SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
            new DbUtils.StatementCallback()
            {
                public void setParameters(PreparedStatement statement) throws Exception
                {
                    statement.setObject(1, id);
                }

                public void processResult(ResultSet resultSet) throws Exception
                {
                    if (resultSet.next())
                    {
                        assertEquals(fieldData, resultSet.getBoolean(getFieldName(entityType, getterName)));
                    }
                    else
                    {
                        fail("No entry found in database with ID " + id);
                    }
                }
            }
        );
    }
}