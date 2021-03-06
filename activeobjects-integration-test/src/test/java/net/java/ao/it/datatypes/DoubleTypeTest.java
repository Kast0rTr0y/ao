package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
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
import net.java.ao.util.DoubleUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for Double data type
 */
public final class DoubleTypeTest extends ActiveObjectsIntegrationTest {
    /**
     * Test AutoIncrement - not supported
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testAutoIncrement() throws Exception {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * Test PK throws
     */
    @Test(expected = ActiveObjectsException.class)
    @NonTransactional
    public void testSimpleId() throws Exception {
        entityManager.migrate(SimpleId.class);

        final Double id = 1d;
        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", id));

        entityManager.flushAll();
        assertEquals(id, e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", id);
    }

    /**
     * Test valid minimum double value
     */
    @Test
    @NonTransactional
    public void testValidMinValue() throws Exception {
        entityManager.migrate(SimpleColumn.class);
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(DoubleUtils.MIN_VALUE);
        e.save();
        entityManager.flushAll();

        assertEquals(new Double(DoubleUtils.MIN_VALUE), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", DoubleUtils.MIN_VALUE);
    }

    /**
     * Test valid maximum double value
     */
    @Test
    @NonTransactional
    public void testValidMaxValue() throws Exception {
        entityManager.migrate(SimpleColumn.class);
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(DoubleUtils.MAX_VALUE);
        e.save();
        entityManager.flushAll();

        assertEquals(new Double(DoubleUtils.MAX_VALUE), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", DoubleUtils.MAX_VALUE);
    }

    /**
     * Test invalid minimum double value
     */
    @Test(expected = ActiveObjectsException.class)
    @NonTransactional
    public void testInvalidMinValue() throws Exception {
        double badMin = -1.7976931348623157e+308;
        entityManager.migrate(SimpleColumn.class);
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(badMin);
        e.save();
        entityManager.flushAll();

        assertEquals(new Double(badMin), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", badMin);
    }

    /**
     * Test invalid maximum double value
     */
    @Test(expected = ActiveObjectsException.class)
    @NonTransactional
    public void testInvalidMaxValue() throws Exception {
        entityManager.migrate(SimpleColumn.class);
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(Double.MAX_VALUE);
        e.save();
        entityManager.flushAll();

        assertEquals(new Double(Double.MAX_VALUE), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", Double.MAX_VALUE);
    }

    /**
     * Test different values for an Double column (ID column in this case)
     */
    @Test
    @NonTransactional
    public void testSpecialValues() throws Exception {
        entityManager.migrate(SimpleColumn.class);
        // create a row with normal id
        for (Double value : new Double[]{-1.8d, 0d, 1.5d}) {
            SimpleColumn e = entityManager.create(SimpleColumn.class);
            e.setAge(value);
            e.save();
            entityManager.flushAll();

            assertEquals(value, e.getAge());
            checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", value);
        }
    }

    /**
     * Test a simple Double column
     */
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getAge());

        // set
        e.setAge(10.7d);
        e.save();
        entityManager.flushAll();

        assertEquals(new Double(10.7), e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", 10.7d);
    }

    /**
     * Empty String default value should not pass
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Non-Double default value should not pass
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testInvalidDefaultColumn() throws Exception {
        entityManager.migrate(InvalidDefaultColumn.class);
    }

    /**
     * Test default value
     */
    @Test
    @NonTransactional
    public void testDefaultColumn() throws Exception {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();
        assertEquals(new Double(100.2d), e.getAge());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getAge", 100.2d);
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithCreate() throws Exception {
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
    public void testNullColumnWithSet() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getAge"), 23d));
        e.setAge(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getAge());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getAge", null);
    }

    @Test
    @NonTransactional
    public void testNullValueWithPullFromDatabase() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn newEntity = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getAge"), 23d));
        newEntity.setAge(null);
        newEntity.save();

        //Use PullFromDatabase of DoubleType
        SimpleColumn loadedEntity = entityManager.get(SimpleColumn.class, newEntity.getID());
        assertNull(loadedEntity.getAge());
    }

    /**
     * Test a not null column
     */
    @Test
    @NonTransactional
    public void testNotNullColumn() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 20.2));

        entityManager.flushAll();
        assertEquals(new Double(20.2d), e.getAge());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getAge", 20.2d);
    }

    /**
     * Creating an entry without specifying a not-null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNoValue() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class);
    }

    /**
     * Null is not a valid not value for a not null column
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNullValue() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), null));
    }


    /**
     * Primitive column without not null constraint
     */
    @Test
    @NonTransactional
    public void testPrimitiveColumn() throws Exception {
        entityManager.migrate(PrimitiveColumn.class);

        // create
        PrimitiveColumn e = entityManager.create(PrimitiveColumn.class);

        // Check the value which hasn't been set on create
        entityManager.flushAll();
        assertEquals(0d, e.getAge(), 0);
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 0d);

        // set
        e.setAge(10.1d);
        e.save();
        entityManager.flushAll();

        assertEquals(10.1d, e.getAge(), 0);
        checkFieldValue(PrimitiveColumn.class, "getID", e.getID(), "getAge", 10.1d);
    }

    /**
     * Primitive column with not null constraint
     */
    @Test
    @NonTransactional
    public void testPrimitiveNotNullColumn() throws Exception {
        entityManager.migrate(PrimitiveNotNullColumn.class);

        // create
        PrimitiveNotNullColumn e = entityManager.create(PrimitiveNotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getAge"), 10.1d));

        entityManager.flushAll();
        assertEquals(10.1d, e.getAge(), 0);
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 10.1d);

        // set
        e.setAge(20.9d);
        e.save();

        entityManager.flushAll();
        assertEquals(20.9d, e.getAge(), 0);
        checkFieldValue(PrimitiveNotNullColumn.class, "getID", e.getID(), "getAge", 20.9d);
    }

    /**
     * Test deletion
     */
    @Test
    @NonTransactional
    public void testDeletion() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class);
        e.setAge(12345.2d);
        e.save();

        entityManager.delete(e);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Double fieldValue) throws Exception {
        DbUtils.executeStatement(entityManager, "SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
                new DbUtils.StatementCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception {
                        statement.setObject(1, id);
                    }

                    public void processResult(ResultSet resultSet) throws Exception {
                        if (resultSet.next()) {
                            double dbValue = resultSet.getDouble(getFieldName(entityType, getterName));
                            assertEquals(fieldValue, resultSet.wasNull() ? null : dbValue);
                        } else {
                            fail("No entry found in database with ID " + id);
                        }
                    }
                }
        );
    }

    /**
     * AutoIncrement primary key
     */
    public static interface AutoIncrementId extends RawEntity<Double> {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Double getId();
    }

    /**
     * Simple primary key
     */
    public static interface SimpleId extends RawEntity<Double> {
        @PrimaryKey("ID")
        public Double getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity {
        public Double getAge();

        public void setAge(Double age);
    }

    /**
     * Invalid default value - not a number
     */
    public static interface EmptyDefaultColumn extends Entity {
        @Default("")
        public Double getAge();

        public void setAge(Double age);
    }

    /**
     * Invalid value default column - not a number
     */
    public static interface InvalidDefaultColumn extends Entity {
        @Default("Test")
        public Double getAge();

        public void setAge(Double age);
    }

    /**
     * Default value column
     */
    public static interface DefaultColumn extends Entity {
        @Default("100.2d")
        public Double getAge();

        public void setAge(Double age);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity {
        @NotNull
        public Double getAge();

        public void setAge(Double age);
    }

    /**
     * Primitive column
     */
    public static interface PrimitiveColumn extends Entity {
        public double getAge();

        public void setAge(double age);
    }

    /**
     * Primitive not null column
     */
    public static interface PrimitiveNotNullColumn extends Entity {
        @NotNull
        public double getAge();

        public void setAge(double age);
    }
}
