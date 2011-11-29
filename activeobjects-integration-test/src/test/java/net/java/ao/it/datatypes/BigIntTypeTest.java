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
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * Tests for BigInt data type
 */
public final class BigIntTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * Test AutoIncrement
     */
    @Test
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);

        AutoIncrementId e = entityManager.create(AutoIncrementId.class);

        entityManager.flushAll();
        assertEquals(new Long(1), e.getId());
        checkFieldValue(AutoIncrementId.class, "getId", e.getId(), "getId", 1l);
    }

    /**
     * Test simple creation
     */
    @Test
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", 12345l));
        entityManager.flushAll();
        assertEquals(new Long(12345), e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", 12345l);
    }

    /**
     * Null should not be a valid id value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullId() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Test different values for an Long column (ID column in this case)
     */
    @Test
    public void testSpecialIds() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        // create a row with normal id
        for (Long value : new Long[] {Long.MIN_VALUE, -1l, 0l, 1l, Long.MAX_VALUE })
        {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", value));
            assertEquals(value, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", value);
        }
    }

    /**
     * Test a simple Long column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getAge());

        // set
        e.setAge(10l);
        e.save();
        entityManager.flushAll();

        assertEquals(new Long(10), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", 10l);
    }

    /**
     * Empty String default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field, not a
     *           NumberFormatException
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Non-Long default value should not pass
     * Expected: A ConfigurationException telling that the provided value is invalid for the given field, not a
     *           NumberFormatException
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);
    }

    /**
     * Test default value
     */
    @Test
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(new Long(100), e.getAge());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getAge", 100l);
    }

    /**
     * Test a not null column
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 20));

        entityManager.flushAll();
        assertEquals(new Long(20), e.getAge());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getAge", 20l);
    }

    /**
     * Creating an entry without specifying a not-null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnNoValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class);
    }

    /**
     * Null is not a valid not value for a not null column
     */
    @Test(expected = IllegalArgumentException.class)
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
    public void testPrimitiveColumn() throws Exception
    {
        entityManager.migrate(PrimitiveColumn.class);

        // create
        PrimitiveColumn e = entityManager.create(PrimitiveColumn.class);

        // Check the value which hasn't been set on create
        entityManager.flushAll();
        assertEquals(0l, e.getAge());
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 0l);

        // set
        e.setAge(10l);
        e.save();
        entityManager.flushAll();

        assertEquals(10l, e.getAge());
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 10l);
    }

    /**
     * Primitive column with not null constraint
     */
    @Test
    public void testPrimitiveNotNullColumn() throws Exception
    {
        entityManager.migrate(PrimitiveNotNullColumn.class);

        // create
        PrimitiveNotNullColumn e = entityManager.create(PrimitiveNotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 10l));

        entityManager.flushAll();
        assertEquals(10l, e.getAge());
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 10l);

        // set
        e.setAge(20l);
        e.save();

        entityManager.flushAll();
        assertEquals(20l, e.getAge());
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 20l);
    }


    /**
     * Test deletion
     */
    @Test
    public void testDeletion() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", 12345l));
        entityManager.delete(e);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Long fieldValue) throws Exception
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
                            assertEquals(fieldValue, (Long) resultSet.getLong(getFieldName(entityType, getterName)));
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
     * AutoIncrement primary key
     */
    public static interface AutoIncrementId extends RawEntity<Long>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Long getId();
    }

    /**
     * Simple primary key
     */
    public static interface SimpleId extends RawEntity<Long>
    {
        @PrimaryKey("ID")
        public Long getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        public Long getAge();
        public void setAge(Long age);
    }

    /**
     * Invalid default value - not a number
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Long getAge();
        public void setAge(Long age);
    }

    /**
     * Invalid value default column - not a number
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Long getAge();
        public void setAge(Long age);
    }

    /**
     * Default value column
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("100")
        public Long getAge();
        public void setAge(Long age);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Long getAge();
        public void setAge(Long age);
    }

    /**
     * Primitive column
     */
    public static interface PrimitiveColumn extends Entity
    {
        public long getAge();
        public void setAge(long age);
    }

    /**
     * Primitive not null column
     */
    public static interface PrimitiveNotNullColumn extends Entity
    {
        @NotNull
        public long getAge();
        public void setAge(long age);
    }
}
