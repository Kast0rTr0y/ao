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

import org.junit.Ignore;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

/**
 * Tests for Enum data type
 */
@SuppressWarnings("unchecked")
public final class EnumTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * Test AutoIncrement - this must not work on enums
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * Test simple creation
     */
    @Test
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam(getFieldName(SimpleId.class, "getId"), Data.FIRST));
        entityManager.flushAll();
        assertEquals(Data.FIRST, e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", Data.FIRST);
    }

    /**
     * Null should not be a valid id value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam(getFieldName(SimpleId.class, "getId"), null));
    }

    /**
     * Test different values for an enum column (ID column in this case)
     */
    @Test
    public void testSpecialIds() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        for (Data data : Data.values())
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam(getFieldName(SimpleId.class, "getId"), data));
            assertEquals(data, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", data);
        }
    }

    /**
     * Test a simple enum column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getData());

        // set
        e.setData(Data.FIRST);
        e.save();
        entityManager.flushAll();

        assertEquals(Data.FIRST, e.getData());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getData", Data.FIRST);
    }

    /**
     * Empty String default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Non-Enum default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);
    }

    /**
     * out of range Enum default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testOutOfRangeDefaultColumn() throws Exception
    {
        entityManager.migrate(OutOfRangeDefaultColumn.class);
    }

    /**
     * Test default value
     */
    @Test
    @Ignore("Parsing of enum values from strings is not yet supported")
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(Data.SECOND, e.getData());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getData", Data.SECOND);
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithCreate() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getData"), null));

        entityManager.flushAll();
        assertNull(e.getData());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getData", null);
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithSet() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getData"), Data.SECOND));
        e.setData(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getData());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getData", null);
    }

    /**
     * Test a not null column
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getData"), Data.THIRD));
        entityManager.flushAll();

        assertEquals(Data.THIRD, e.getData());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getData", Data.THIRD);
    }

    /**
     * Creating an entry without specifying a not-null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNoValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class);
    }

    /**
     * Null is not a valid value for a not null column
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNullValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getData"), null));
    }

    /**
     * Test deletion
     */
    @Test
    public void testDeletion() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", Data.THIRD));
        entityManager.delete(e);

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

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Data fieldValue) throws Exception
    {
        DbUtils.executeStatement(entityManager, "SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
                new DbUtils.StatementCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        if (id instanceof Enum<?>)
                        {
                            statement.setString(1, ((Enum<?>) id).name());
                        }
                        else
                        {
                            statement.setObject(1, id);
                        }
                    }

                    public void processResult(ResultSet resultSet) throws Exception {
                        if (resultSet.next()) {
                            String dbValue = resultSet.getString(getFieldName(entityType, getterName));
                            assertEquals(fieldValue == null ? null : fieldValue.name(), resultSet.wasNull() ? null : dbValue);
                        } else {
                            fail("No entry found in database with ID " + id);
                        }
                    }
                }
        );
    }

    public static enum Data {
        FIRST, SECOND, THIRD
    }

    /**
     * AutoIncrement primary key
     */
    public static interface AutoIncrementId extends RawEntity<Data>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Data getId();
    }

    /**
     * Simple primary key
     */
    public static interface SimpleId extends RawEntity<Data>
    {
        @PrimaryKey("ID")
        public Data getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        public Data getData();
        public void setData(Data data);
    }

    /**
     * Invalid default value - not matching the enum
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Data getData();
        public void setData(Data data);
    }

    /**
     * Invalid value default column - not matching the enum
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Data getData();
        public void setData(Data data);
    }

    /**
     * Out of range value default column
     */
    public static interface OutOfRangeDefaultColumn extends Entity
    {
        @Default("99")
        public Data getData();
        public void setData(Data data);
    }

    /**
     * Default value column
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("1")
        public Data getData();
        public void setData(Data data);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Data getData();
        public void setData(Data data);
    }

}
