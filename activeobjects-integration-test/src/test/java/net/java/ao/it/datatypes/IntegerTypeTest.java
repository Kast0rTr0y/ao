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
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * Tests for Integer data type
 */
public final class IntegerTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * Test AutoIncrement
     */
    @Test
    @NonTransactional
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);

        AutoIncrementId e = entityManager.create(AutoIncrementId.class);

        entityManager.flushAll();
        assertEquals(new Integer(1), e.getId());
        checkFieldValue(AutoIncrementId.class, "getId", e.getId(), "getId", 1);
    }

    /**
     * Test simple creation
     */
    @Test
    @NonTransactional
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", 12345));
        entityManager.flushAll();
        assertEquals(new Integer(12345), e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", 12345);
    }

    /**
     * Null not a valid id value
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Test different values for an Integer column (ID column in this case)
     */
    @Test
    @NonTransactional
    public void testSpecialIds() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        // create a row with normal id
        for (Integer value : new Integer[]{Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", value));
            assertEquals(value, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", value);
        }
    }

    /**
     * Test a simple Integer column
     */
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getAge());

        // set
        e.setAge(10);
        e.save();
        entityManager.flushAll();

        assertEquals(new Integer(10), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", 10);
    }

    /**
     * Empty String default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field, not a
     * NumberFormatException
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Non-integer default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field, not a
     * NumberFormatException
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);
    }

    /**
     * Test default value
     */
    @Test
    @NonTransactional
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(new Integer(100), e.getAge());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getAge", 100);
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
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getAge"), null));

        entityManager.flushAll();
        assertNull(e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", null);
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
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getAge"), 23));
        e.setAge(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", null);
    }

    @Test
    @NonTransactional
    public void testNullValueWithPullFromDatabase() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn newEntity = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getAge"), 23));
        newEntity.setAge(null);
        newEntity.save();

        entityManager.flushAll();

        //Use PullFromDatabase of IntegerType
        SimpleColumn loadedEntity = entityManager.get(SimpleColumn.class, newEntity.getID());
        assertNull(loadedEntity.getAge());
    }

    /**
     * Test a not null column
     */
    @Test
    @NonTransactional
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 20));

        entityManager.flushAll();
        assertEquals(new Integer(20), e.getAge());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getAge", 20);
    }

    /**
     * Test setting null in not null column
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnSetNull() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 20));

        entityManager.flushAll();

        e.setAge(null);
    }

    /**
     * Creating an entry without specifying a not-null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNoValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class);
    }

    /**
     * Null is not a valid value for a not null column
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNullValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), null));
    }

    /**
     * Primitive column without not null constraint
     */
    @Test
    @NonTransactional
    public void testPrimitiveColumn() throws Exception
    {
        entityManager.migrate(PrimitiveColumn.class);

        // create
        PrimitiveColumn e = entityManager.create(PrimitiveColumn.class);

        // Check the value which hasn't been set on create
        entityManager.flushAll();
        assertEquals(0, e.getAge());
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 0);

        // set
        e.setAge(10);
        e.save();
        entityManager.flushAll();

        assertEquals(10, e.getAge());
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 10);
    }

    /**
     * Primitive column with not null constraint
     */
    @Test
    @NonTransactional
    public void testPrimitiveNotNullColumn() throws Exception
    {
        entityManager.migrate(PrimitiveNotNullColumn.class);

        // create
        PrimitiveNotNullColumn e = entityManager.create(PrimitiveNotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 10));

        entityManager.flushAll();
        assertEquals(10, e.getAge());
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 10);

        // set
        e.setAge(20);
        e.save();

        entityManager.flushAll();
        assertEquals(20, e.getAge());
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 20);
    }

    /**
     * Test deletion
     */
    @Test
    @NonTransactional
    public void testDelete() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", 12345));
        entityManager.delete(e);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Integer fieldValue) throws Exception
    {
        DbUtils.executeStatement(entityManager, "SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
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
                            int dbValue = resultSet.getInt(getFieldName(entityType, getterName));
                            assertEquals(fieldValue, resultSet.wasNull() ? null : dbValue);
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
     * AutoIncrement primary key column
     */
    public static interface AutoIncrementId extends RawEntity<Integer>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Integer getId();
    }

    /**
     * Simple primary key column
     */
    public static interface SimpleId extends RawEntity<Integer>
    {
        @PrimaryKey("ID")
        public Integer getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        public Integer getAge();
        public void setAge(Integer age);
    }

    /**
     * Empty default column
     * Invalid as not supported accross all databases
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Integer getAge();
        public void setAge(Integer age);
    }

    /**
     * Invalid default value - must be parseable to an Integer
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Integer getAge();
        public void setAge(Integer age);
    }

    /**
     * Valid default value
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("100")
        public Integer getAge();
        public void setAge(Integer age);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Integer getAge();
        public void setAge(Integer age);
    }

    /**
     * Primitive column
     */
    public static interface PrimitiveColumn extends Entity
    {
        public int getAge();
        public void setAge(int age);
    }

    /**
     * Primitive not null column
     */
    public static interface PrimitiveNotNullColumn extends Entity
    {
        @NotNull
        public int getAge();
        public void setAge(int age);
    }
}
