package net.java.ao.it.datatypes;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import net.java.ao.*;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.Default;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.EntityUtils;
import net.java.ao.util.DateUtils;

import static org.junit.Assert.*;

/**
 * Tests for Date data type (mapped to the Date class)
 */
public final class TimestampDateTypeTest extends ActiveObjectsIntegrationTest
{
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Test AutoIncrement - not supported
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testAutoIncrement() throws Exception
    {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * Date is not allowed as a primary key.
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testSimpleId() throws Exception
    {
        entityManager.migrate(SimpleId.class);
    }

    @Test(expected = ActiveObjectsException.class)
    @NonTransactional
    public void testDateTooFar() throws Exception
    {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", new Date(Long.MAX_VALUE)));
    }

    /**
     * Test different values for an Date column
     */
    @Test
    @NonTransactional
    public void testSpecialIds() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create a row with normal id
        for (Date value : new Date[] {new Date(0), new Date(), DateUtils.MAX_DATE })
        {
            SimpleColumn e = entityManager.create(SimpleColumn.class);
            e.setCreated(value);
            e.save();

            entityManager.flushAll();

            Calendar expected = Calendar.getInstance();
            expected.setTime(value);

            Calendar actual = Calendar.getInstance();
            actual.setTime(e.getCreated());

            expected.set(Calendar.MILLISECOND, 0);
            actual.set(Calendar.MILLISECOND, 0);

            assertEquals(expected.getTime(),actual.getTime());

            checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getCreated", value);
        }
    }

    /**
     * Test a simple Date column
     */
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getCreated());

        // set
        Date date = new Date();
        e.setCreated(date);
        e.save();

        entityManager.flushAll();

        Calendar expected = Calendar.getInstance();
        expected.setTime(date);

        Calendar actual = Calendar.getInstance();
        actual.setTime(e.getCreated());

        expected.set(Calendar.MILLISECOND, 0);
        actual.set(Calendar.MILLISECOND, 0);

        assertEquals(expected.getTime(),actual.getTime());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getCreated", date);
    }

    /**
     * Empty String default value not a valid date
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Invalid default value
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testInvalidDefaultColumn() throws Exception
    {
        entityManager.migrate(InvalidDefaultColumn.class);
    }

    /**
     * Valid default value
     */
    @Test
    @NonTransactional
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        entityManager.flushAll();

        Date date = new SimpleDateFormat(DATE_FORMAT).parse("2011-11-11 12:34:56");

        Calendar expected = Calendar.getInstance();
        expected.setTime(date);

        Calendar actual = Calendar.getInstance();
        actual.setTime(e.getCreated());

        expected.set(Calendar.MILLISECOND, 0);
        actual.set(Calendar.MILLISECOND, 0);

        assertEquals(expected.getTime(),actual.getTime());

        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getCreated", date);
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
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getCreated"), null));

        entityManager.flushAll();
        assertNull(e.getCreated());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getCreated", null);
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithSet() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        Date date = new SimpleDateFormat(DATE_FORMAT).parse("2011-11-11 12:34:56");
        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getCreated"), date));
        e.setCreated(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getCreated());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getCreated", null);
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
        Date date = new Date();
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getCreated"), date));

        entityManager.flushAll();

        Calendar expected = Calendar.getInstance();
        expected.setTime(date);

        Calendar actual = Calendar.getInstance();
        actual.setTime(e.getCreated());

        expected.set(Calendar.MILLISECOND, 0);
        actual.set(Calendar.MILLISECOND, 0);

        assertEquals(expected.getTime(),actual.getTime());
        
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getCreated", date);
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
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getCreated"), new Date()));

        entityManager.flushAll();
        e.setCreated(null);
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
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getCreated"), null));
    }

    /**
     * Test deletion
     */
    @Test
    @NonTransactional
    public void testDelete() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getCreated());

        // set
        Date date = new Date();
        e.setCreated(date);
        e.save();

        entityManager.flushAll();
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getCreated", date);

        entityManager.delete(e);
        entityManager.flushAll();

        executeStatement("SELECT * FROM " + EntityUtils.getTableName(entityManager, SimpleColumn.class), new DbUtils.StatementCallback()
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

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final Date fieldValue) throws Exception
    {
        DbUtils.executeStatement(entityManager, "SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement statement) throws Exception
                    {
                        if (id instanceof Date)
                        {
                            statement.setTimestamp(1, new java.sql.Timestamp(((Date) id).getTime()));
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
                            if (fieldValue == null)
                            {
                                resultSet.getTimestamp(getFieldName(entityType, getterName));
                                assertTrue(resultSet.wasNull());
                            }
                            else
                            {
                                final DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT); // this date format doesn't include millis.
                                assertEquals(dateFormat.format(new Date(fieldValue.getTime())), dateFormat.format(resultSet.getTimestamp(getFieldName(entityType, getterName))));
                            }
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
     * AutoIncrement primary key column - not supported
     */
    public static interface AutoIncrementId extends RawEntity<Date>
    {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public Date getId();
    }

    /**
     * Simple primary key column
     */
    public static interface SimpleId extends RawEntity<Date>
    {
        @PrimaryKey("ID")
        public Date getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity
    {
        public Date getCreated();
        public void setCreated(Date created);
    }

    /**
     * Empty default column - invalid date
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public Date getCreated();
        public void setCreated(Date created);
    }

    /**
     * Invalid default value - must be of the form yyyy-MM-dd
     */
    public static interface InvalidDefaultColumn extends Entity
    {
        @Default("Test")
        public Date getCreated();
        public void setCreated(Date created);
    }

    /**
     * Valid default value
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("2011-11-11 12:34:56")
        public Date getCreated();
        public void setCreated(Date created);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity
    {
        @NotNull
        public Date getCreated();
        public void setCreated(Date created);
    }
}
