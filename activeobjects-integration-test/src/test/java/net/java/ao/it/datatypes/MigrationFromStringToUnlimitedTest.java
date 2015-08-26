package net.java.ao.it.datatypes;

import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseProvider;
import net.java.ao.RawEntity;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.sql.PreparedStatement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class MigrationFromStringToUnlimitedTest extends ActiveObjectsIntegrationTest {

    private static String LARGE_STRING;
    private static final String MAX_LENGTH_STRING;

    static {
        String s = "123456789#"; // 10 chars
        StringBuilder sb = new StringBuilder(s.length() * 600);
        for (int i = 0; i < 600; i++) {
            sb.append(s);
        }
        sb.append(sb.length() + 4);
        LARGE_STRING = sb.toString();
        MAX_LENGTH_STRING = sb.substring(0, StringLength.MAX_LENGTH);
    }

    @Test
    @NonTransactional
    public void testMigration() throws Exception {
        entityManager.migrate(VarcharColumn.class);

        final VarcharColumn e = entityManager.create(VarcharColumn.class,
                new DBParam("ID", 1));
        e.setText(MAX_LENGTH_STRING);
        e.save();

        entityManager.migrate(LargeTextColumn.class);

        LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, e.getID());
        assertEquals(MAX_LENGTH_STRING, retrieved.getText());

        retrieved.setText(LARGE_STRING);
        retrieved.save();
    }

    @Test
    @NonTransactional
    public void testMigrationFromOversizedColumn() throws Exception {
        DatabaseProvider provider = entityManager.getProvider();
        String primaryKeyType = provider.getTypeManager().getType(Common.getPrimaryKeyClassType(VarcharColumn.class)).getSqlTypeIdentifier();
        String tableName = provider.shorten(entityManager.getTableNameConverter().getName(VarcharColumn.class));
        createTable(provider, primaryKeyType, tableName);
        insertTextIntoTable(provider, tableName);
        entityManager.migrate(LargeTextColumn.class);
        entityManager.create(LargeTextColumn.class,
                new DBParam("ID", 2), new DBParam("TEXT", LARGE_STRING));
        LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, 1);
        assertThat(retrieved.getText(), equalTo(MAX_LENGTH_STRING));
        retrieved = entityManager.get(LargeTextColumn.class, 2);
        assertThat(retrieved.getText(), equalTo(LARGE_STRING));
    }

    private void createTable(final DatabaseProvider provider, final String primaryKeyType, final String tableName)
            throws Exception {
        DbUtils.executeUpdate(entityManager,
                "CREATE TABLE " + provider.withSchema(tableName) + "(" + provider.processID("ID") + " " + primaryKeyType + " NOT NULL, "
                        + provider.processID("TEXT") + " VARCHAR(767)," + "CONSTRAINT " + "pk_" + tableName
                        + "_ID PRIMARY KEY (" + provider.processID("ID") + "))",
                new DbUtils.UpdateCallback() {
                    @Override
                    public void setParameters(final PreparedStatement statement) throws Exception {

                    }
                });
    }

    private void insertTextIntoTable(final DatabaseProvider provider, final String tableName) throws Exception {
        DbUtils.executeUpdate(entityManager,
                "INSERT INTO " + provider.withSchema(tableName) + "(" + provider.processID("ID") + ","
                        + provider.processID("TEXT") + " )" + " VALUES (?, ?)",

                new DbUtils.UpdateCallback() {
                    @Override
                    public void setParameters(final PreparedStatement statement) throws Exception {
                        statement.setInt(1, 1);
                        statement.setString(2, MAX_LENGTH_STRING);
                    }
                });
    }

    @Table("ENTITY")
    public static interface LargeTextColumn extends RawEntity<Integer> {
        @NotNull
        @PrimaryKey("ID")
        public int getID();

        void setId(int id);

        @StringLength(StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    @Table("ENTITY")
    public static interface VarcharColumn extends RawEntity<Integer> {
        @NotNull
        @PrimaryKey("ID")
        public int getID();

        void setId(int id);

        @StringLength(StringLength.MAX_LENGTH)
        public String getText();

        public void setText(String text);
    }

}
