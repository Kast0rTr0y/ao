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
import org.junit.Ignore;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * Tests for Float data type
 */
public final class FloatTypeTest extends ActiveObjectsIntegrationTest
{
    /**
     * Test AutoIncrement - not supported
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * Test simple creation
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);
    }

    /**
     * Test different values for an Float column (ID column in this case)
     */
    @Test
    @Ignore("Need to deal with special values!")
    public void testSpecialValues() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create a row with normal id
        for (Float value : new Float[] {Float.MIN_VALUE, -1.5f, 0f, 1.5f, Float.MAX_VALUE })
        {
            SimpleColumn e = entityManager.create(SimpleColumn.class);
            e.setAge(value);
            e.save();
            assertEquals(value, e.getAge());
            checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", value);
        }
    }

    /**
     * Test a simple Float column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getAge());

        // set
        e.setAge(10.7f);
        e.save();
        entityManager.flushAll();

        assertEquals(new Float(10.7), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", 10.7f);
    }

    /**
     * Empty String default value should not pass
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Non-Float default value should not pass
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
        assertEquals(new Float(100.2f), e.getAge());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getAge", 100.2f);
    }

    /**
     * Test a not null column
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 20.2f));

        entityManager.flushAll();
        assertEquals(new Float(20.2f), e.getAge());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getAge", 20.2f);
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
        assertEquals(0f, e.getAge(), 0);
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 0f);

        // set
        e.setAge(10.1f);
        e.save();
        entityManager.flushAll();

        assertEquals(10.1f, e.getAge(), 0);
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 10.1f);
    }

    /**
     * Primitive column with not null constraint
     */
    @Test
    public void testPrimitiveNotNullColumn() throws Exception
    {
        entityManager.migrate(PrimitiveNotNullColumn.class);

        // create
        PrimitiveNotNullColumn e = entityManager.create(PrimitiveNotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 10.1f));

        entityManager.flushAll();
        assertEquals(10.1f, e.getAge(), 0);
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 10.1f);

        // set
        e.setAge(20.9f);
        e.save();

        entityManager.flushAll();
        assertEquals(20.9f, e.getAge(), 0);
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 20.9f);
    }


    /**
     * Test deletion
     */
    @Test
    public void testDeletion() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(12345.2f);
        e.save();

        entityManager.delete(e);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String isGetterName, final Object id, final String getterName, final Float fieldValue) throws Exception
    {
        DbUtils.executeStatement(entityManager, "SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, isGetterName) + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        if (id instanceof Float)
                        {

                            statement.setFloat(1, (Float) id);
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
                            assertEquals(fieldValue, (Float) resultSet.getFloat(getFieldName(entityType, getterName)));
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
    public static interface AutoIncrementId extends RawEntity<Float>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Float getId();
    }

    /**
     * Simple primary key
     */
    public static interface SimpleId extends RawEntity<Float>
    {
        @PrimaryKey("ID")
        public Float getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        public Float getAge();
        public void setAge(Float age);
    }

    /**
     * Invalid default value - not a number
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Float getAge();
        public void setAge(Float age);
    }

    /**
     * Invalid value default column - not a number
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Float getAge();
        public void setAge(Float age);
    }

    /**
     * Default value column
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("100.2")
        public Float getAge();
        public void setAge(Float age);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Float getAge();
        public void setAge(Float age);
    }

    /**
     * Primitive column
     */
    public static interface PrimitiveColumn extends Entity
    {
        public float getAge();
        public void setAge(float age);
    }

    /**
     * Primitive not null column
     */
    public static interface PrimitiveNotNullColumn extends Entity
    {
        @NotNull
        public float getAge();
        public void setAge(float age);
    }

}
