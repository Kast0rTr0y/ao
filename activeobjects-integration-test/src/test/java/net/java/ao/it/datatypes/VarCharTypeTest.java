package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.Default;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.StringLength;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.types.TypeQualifiers;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * VarChar data type specific tests
 */
@SuppressWarnings("unchecked")
public final class VarCharTypeTest extends ActiveObjectsIntegrationTest {
    /**
     * VarChar does not support AutoIncrement
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testAutoIncrement() throws Exception {
        entityManager.migrate(AutoIncrementId.class);
    }

    /**
     * {@code @NotNull} not required for primary id column
     */
    @Test
    @NonTransactional
    public void testPrimaryWithoutNotNull() throws Exception {
        entityManager.migrate(PrimaryWithoutNotNull.class);
    }

    /**
     * Test creation using a String id
     */
    @Test
    @NonTransactional
    public void testSimpleId() throws Exception {
        entityManager.migrate(SimpleId.class);

        SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", "Test"));
        assertEquals("Test", e.getId());
        checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", "Test");
    }

    /**
     * Empty String is treated as null on some databases, thus we expect an exception here
     */
    @Test(expected = ActiveObjectsException.class)
    @NonTransactional
    public void testEmptyId() throws Exception {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", ""));
    }

    /**
     * Null can't be used as id
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNullId() throws Exception {
        entityManager.migrate(SimpleId.class);

        entityManager.create(SimpleId.class, new DBParam("ID", null));
    }

    /**
     * Test different values for a VarChar column (ID column in this case)
     */
    @Test
    @NonTransactional
    public void testColumnValues() throws Exception {
        entityManager.migrate(SimpleId.class);

        // create a row with normal id
        for (String value : new String[]{"TABLE", "COLUMN", "NULL", "INDEX", "PRIMARY", "WHERE", "SELECT", "FROM", "@", ",", ";"}) {
            SimpleId e = entityManager.create(SimpleId.class, new DBParam("ID", value));
            assertEquals(value, e.getId());
            checkFieldValue(SimpleId.class, "getId", e.getId(), "getId", value);
        }
    }

    /**
     * Update a simple varchar column
     */
    @Test
    @NonTransactional
    public void testSimpleColumn() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getName());

        // set
        e.setName("Test");
        e.save();

        assertEquals("Test", e.getName());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getName", "Test");
    }

    /**
     * Empty string is treated as null on certain databases, disallow
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Check different default values
     */
    @Test
    @NonTransactional
    public void testDefaultColumn() throws Exception {
        entityManager.migrate(DefaultColumn.class);

        // create
        DefaultColumn e = entityManager.create(DefaultColumn.class);

        assertEquals("Test", e.getName());
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getName", "Test");
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getName2", "NULL");
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getName3", "ID");
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getName4", "AutoIncrement");
        checkFieldValue(DefaultColumn.class, "getID", e.getID(), "getName5", "TEST); -- DROP DATABASE *");
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithCreate() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getName"), null));

        assertNull(e.getName());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getName", null);
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testNullColumnWithSet() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getName"), "Test"));
        e.setName(null);
        e.save();

        assertNull(e.getName());
        checkFieldValue(SimpleColumn.class, "getID", e.getID(), "getName", null);
    }

    @Test
    @NonTransactional
    public void testNullValueWithPullFromDatabase() throws Exception {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn newEntity = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getName"), "Test"));
        newEntity.setName(null);
        newEntity.save();

        //Use PullFromDatabase of StringType
        SimpleColumn loadedEntity = entityManager.get(SimpleColumn.class, newEntity.getID());
        assertNull(loadedEntity.getName());
    }

    /**
     * Test a not null column constraint column
     */
    @Test
    @NonTransactional
    public void testNotNullColumn() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getName"), "Test"));

        assertEquals("Test", e.getName());
        checkFieldValue(NotNullColumn.class, "getID", e.getID(), "getName", "Test");
    }

    /**
     * Not providing a value for a not null column should fail
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNoValue() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        entityManager.create(NotNullColumn.class);
    }

    /**
     * Inserting null in a not null column should throw an Exception
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnNullValue() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getName"), null));
    }

    /**
     * Empty string is treated as null on certain databases. Don't allow storing empty string in a NOTNULL column
     * (passes fine on hsql but blows on MSSQL/Oracle
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnEmptyString() throws Exception {
        entityManager.migrate(NotNullColumn.class);

        entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getName"), ""));
    }

    /**
     * String length can be specified as long as it is within the maximum value for varchars in all databases
     */
    @Test
    @NonTransactional
    public void testColumnWithAllowableLength() throws Exception {
        entityManager.migrate(ColumnWithAllowableLength.class);

        entityManager.create(ColumnWithAllowableLength.class,
                new DBParam(getFieldName(ColumnWithAllowableLength.class, "getName"),
                        StringUtils.repeat("*", TypeQualifiers.MAX_STRING_LENGTH)));
    }

    @Test
    @NonTransactional
    public void testColumnWithAllowableLengthAndIndex() throws Exception {
        entityManager.migrate(ColumnWithAllowableLengthAndIndex.class);

        entityManager.create(ColumnWithAllowableLengthAndIndex.class,
                new DBParam(getFieldName(ColumnWithAllowableLengthAndIndex.class, "getName"),
                        StringUtils.repeat("*", TypeQualifiers.MAX_STRING_LENGTH)));
    }

    /**
     * StringLength annotation that is above the maximum value for varchars in any database causes an error
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testColumnWithExcessiveLength() throws Exception {
        entityManager.migrate(ColumnWithExcessiveLength.class);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Object id, final String getterName, final String fieldValue) throws Exception {
        executeStatement("SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
                new DbUtils.StatementCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception {
                        statement.setObject(1, id);
                    }

                    public void processResult(ResultSet resultSet) throws Exception {
                        if (resultSet.next()) {
                            assertEquals(fieldValue, resultSet.getString(getFieldName(entityType, getterName)));
                        } else {
                            fail("No entry found in database with ID " + id);
                        }
                    }
                }
        );
    }

    /**
     * Auto increment primary key column - not supported
     */
    public static interface AutoIncrementId extends RawEntity<String> {
        @AutoIncrement
        @NotNull
        @PrimaryKey("ID")
        public String getId();
    }

    /**
     * Primary column without NotNull annotation - should be ok
     */
    public static interface PrimaryWithoutNotNull extends RawEntity<String> {
        @PrimaryKey("ID")
        public String getId();
    }

    /**
     * Simple primary column
     */
    public static interface SimpleId extends RawEntity<String> {
        @NotNull
        @PrimaryKey("ID")
        public String getId();
    }

    /**
     * Simple column
     */
    public static interface SimpleColumn extends Entity {
        public String getName();

        public void setName(String name);
    }

    /**
     * Empty String for default value - not supported
     */
    public static interface EmptyDefaultColumn extends Entity {
        @Default("")
        public String getName();

        public void setName(String name);
    }

    /**
     * Default values
     */
    @SuppressWarnings("unused")
    public static interface DefaultColumn extends Entity {
        @Default("Test")
        public String getName();

        public void setName(String name);

        @Default("NULL")
        public String getName2();

        public void setName2(String name);

        @Default("ID")
        public String getName3();

        public void setName3(String name);

        @Default("AutoIncrement")
        public String getName4();

        public void setName4(String name);

        @Default("TEST); -- DROP DATABASE *")
        public String getName5();

        public void setName5(String name);
    }

    /**
     * Not null column
     */
    public static interface NotNullColumn extends Entity {
        @NotNull
        public String getName();

        public void setName(String name);
    }

    /**
     * Column with fixed string length annotation that is within the allowable limit
     */
    public static interface ColumnWithAllowableLength extends Entity {
        @StringLength(TypeQualifiers.MAX_STRING_LENGTH)
        public String getName();

        public void setName(String name);
    }

    public static interface ColumnWithAllowableLengthAndIndex extends Entity {
        @Indexed
        @StringLength(TypeQualifiers.MAX_STRING_LENGTH)
        public String getName();

        public void setName(String name);
    }

    /**
     * Column with fixed string length annotation that is above the allowable limit
     */
    public static interface ColumnWithExcessiveLength extends Entity {
        @StringLength(TypeQualifiers.MAX_STRING_LENGTH + 1)
        public String getName();

        public void setName(String name);
    }
}
