package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.schema.Default;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.HashMap;

import static org.junit.Assert.*;

public final class ClobTypeTest extends ActiveObjectsIntegrationTest
{
    private static String SMALL_CLOB = "Some small sample";

    // over 4000 bytes, as Oracle has issues with that.
    private static String LARGE_CLOB;

    static
    {
        int size = 8100;
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size / 10; i++)
        {
            sb.append("0123456789#");
        }
        LARGE_CLOB = sb.append(size).toString();
    }

    /**
     * Tests that we can insert a string of greather than 2^16 characters in length.
     * see https://ecosystem.atlassian.net/browse/AO-396 for details.
     */
    @Test
    public void testUnlimitedLengthFieldCanStoreStringWithLengthGreaterThan64k() throws Exception
    {
        DDLAction createTableAction = new DDLAction(DDLActionType.CREATE);
        createTableAction.setTable(SchemaGenerator.parseInterface(entityManager.getProvider(), entityManager.getTableNameConverter(), entityManager.getFieldNameConverter(), LargeTextColumn.class));
        Iterable<SQLAction> action = entityManager.getProvider().renderAction(entityManager.getNameConverters(), createTableAction);
        String sqlStatement = action.iterator().next().getStatement();

        if (entityManager.getProvider() instanceof MySQLDatabaseProvider)
        {
            sqlStatement = sqlStatement.replace("TEXT LONGTEXT NOT NULL", "TEXT TEXT NOT NULL"); //force the DDL to create using the TEXT type when in MySQL so we test the migration
        }
        executeUpdate(sqlStatement, new DbUtils.UpdateCallback()
        {
            @Override
            public void setParameters(PreparedStatement statement) throws Exception {}
        });

        entityManager.migrate(LargeTextColumn.class);
        final HashMap<String, Object> params = new HashMap<String, Object>();
        final int size = (int) Math.pow(2, 17);
        params.put(getFieldName(LargeTextColumn.class, "getText"), createString(size));
        LargeTextColumn entity = entityManager.create(LargeTextColumn.class, params);
        entity.save();
        entityManager.flushAll();
        LargeTextColumn[] found = entityManager.find(LargeTextColumn.class);
        assertEquals("should only have inserted 1 row", 1, found.length);
        assertEquals("should be " + size + " long", size, found[0].getText().length());
    }

    private String createString(int size)
    {
        return new String(new char[size]).replace("\0", "a");
    }

    /**
     * Test simple clob column
     */
    @Test
    public void testSimpleColumn() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getText());

        // test small clob
        e.setText(SMALL_CLOB);
        e.save();

        entityManager.flushAll();
        assertEquals(SMALL_CLOB, e.getText());

        // test large clob
        e.setText(LARGE_CLOB);
        e.save();

        entityManager.flushAll();
        assertEquals(LARGE_CLOB, e.getText());

        // test empty clob
        e.setText(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getText());
        // TODO: check database value
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithCreate() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getText"), null));

        entityManager.flushAll();
        assertNull(e.getText());
        // TODO: check database value
    }

    /**
     * Test null value
     */
    @Test
    public void testNullColumnWithSet() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class, new DBParam(getFieldName(SimpleColumn.class, "getText"), SMALL_CLOB));
        e.setText(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getText());
        // TODO: check database value
    }

    /**
     * Test not null column create
     */
    @Test
    public void testNotNullColumn() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getText"), LARGE_CLOB));

        entityManager.flushAll();
        assertEquals(LARGE_CLOB, e.getText());
        // TODO: check database value
    }

    /**
     * Test NotNull column create no value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnCreatingWithoutValue() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        entityManager.create(NotNullColumn.class);
    }

    /**
     * Test NotNull blob column
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNotNullColumnSetNull() throws Exception
    {
        entityManager.migrate(NotNullColumn.class);

        // create
        NotNullColumn e = entityManager.create(NotNullColumn.class, new DBParam(getFieldName(NotNullColumn.class, "getText"), SMALL_CLOB));

        // set value to null should fail
        e.setText(null);
    }

    /**
     * Test default value
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);

        DefaultColumn e = entityManager.create(DefaultColumn.class);
        assertEquals("Test", e.getText());
    }

    /**
     * Empty String default value should not pass, is null on certain dbs
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Test deletion
     */
    @Test
    public void testDeletion() throws Exception
    {
        entityManager.migrate(SimpleColumn.class);

        // create
        SimpleColumn e = entityManager.create(SimpleColumn.class);
        assertNull(e.getText());

        // set data
        e.setText(LARGE_CLOB);
        e.save();

        entityManager.delete(e);

        // TODO: check that blob got deleted
    }

    public static interface SimpleColumn extends Entity
    {
        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    public static interface DefaultColumn extends Entity
    {
        @Default("Test")
        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    public static interface NotNullColumn extends Entity
    {
        @NotNull
        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    public static interface LargeTextColumn extends Entity {
        @NotNull
        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }
}
