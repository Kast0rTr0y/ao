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
import net.java.ao.types.TypeQualifiers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.PreparedStatement;
import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public final class SchemaWarningsForFieldsThatAreTooLongTest extends ActiveObjectsIntegrationTest
{

    private static final String MAX_LENGTH_STRING;
    private static final String TOO_LONG_STRING;
    static
    {
        String s = "123456789a"; // 10 chars
        Random r = new Random();
        StringBuilder sb = new StringBuilder(TypeQualifiers.OLD_MAX_STRING_LENGTH);
        for (int i = 0; i < TypeQualifiers.OLD_MAX_STRING_LENGTH; i++)
        {
            sb.append(s.charAt(r.nextInt(s.length())));
        }
        MAX_LENGTH_STRING = sb.toString();
        TOO_LONG_STRING = MAX_LENGTH_STRING + "a";
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    @NonTransactional
    public void testNewSchemaDefinition() throws Exception
    {
        entityManager.migrate(LargeTextColumn.class);
        final LargeTextColumn e = entityManager.create(LargeTextColumn.class,
                new DBParam("ID", 1));
        e.setText(MAX_LENGTH_STRING);
        e.save();
    }

    @Test
    @NonTransactional
    public void testMigration() throws Exception
    {
        createAndPopulateNewTable();
        entityManager.migrate(LargeTextColumn.class);
        entityManager.create(LargeTextColumn.class,
                new DBParam("ID", 2), new DBParam("TEXT",MAX_LENGTH_STRING));
        LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, 1);
        assertThat(retrieved.getText(), equalTo(MAX_LENGTH_STRING));
        retrieved = entityManager.get(LargeTextColumn.class, 2);
        assertThat(retrieved.getText(), equalTo(MAX_LENGTH_STRING));
    }

    private void createAndPopulateNewTable() throws Exception
    {
        DatabaseProvider provider  = entityManager.getProvider();
        String primaryKeyType = provider.getTypeManager().getType(Common.getPrimaryKeyClassType(LargeTextColumn.class)).getSqlTypeIdentifier();
        String tableName = provider.shorten(entityManager.getTableNameConverter().getName(LargeTextColumn.class));
        createTable(provider, primaryKeyType, tableName);
        insertTextIntoTable(provider, tableName);
    }


    @Test
    @NonTransactional
    public void testAgainstNewlyCreatedSchema() throws Exception
    {
        entityManager.migrate(LargeTextColumn.class);
        LargeTextColumn e = entityManager.create(LargeTextColumn.class,
                new DBParam("ID", 1));
        e.setText(TOO_LONG_STRING);
        if (!DbUtils.isHsql(entityManager))
        {
            expectedException.expect(UndeclaredThrowableException.class);
        }
        e.save();
    }

    @Test
    @NonTransactional
    public void testAgainstMigratedSchema() throws Exception
    {
        createAndPopulateNewTable();
        entityManager.migrate(LargeTextColumn.class);
        LargeTextColumn e = entityManager.get(LargeTextColumn.class, 1);
        assertThat(e.getText(), equalTo(MAX_LENGTH_STRING));
        e.setText(TOO_LONG_STRING);
        if (!DbUtils.isHsql(entityManager))
        {
            expectedException.expect(UndeclaredThrowableException.class);
        }
        e.save();
    }

    private void createTable(final DatabaseProvider provider, final String primaryKeyType, final String tableName)
            throws Exception
    {
        DbUtils.executeUpdate(entityManager,
                "CREATE TABLE " + provider.withSchema(tableName) + "(" + provider.processID("ID") + " " + primaryKeyType + " NOT NULL, "
                        + provider.processID("TEXT") + " VARCHAR(767)," + "CONSTRAINT " + "pk_" + tableName
                        + "_ID PRIMARY KEY (" + provider.processID("ID") + "))",
                new DbUtils.UpdateCallback()
                {
                    @Override
                    public void setParameters(final PreparedStatement statement) throws Exception
                    {

                    }
                });
    }

    private void insertTextIntoTable(final DatabaseProvider provider, final String tableName) throws Exception
    {
        DbUtils.executeUpdate(entityManager,
                "INSERT INTO " + provider.withSchema(tableName) + "(" + provider.processID("ID") + ","
                        + provider.processID("TEXT") + " )" + " VALUES (?, ?)",

                new DbUtils.UpdateCallback()
                {
                    @Override
                    public void setParameters(final PreparedStatement statement) throws Exception
                    {
                        statement.setInt(1, 1);
                        statement.setString(2, MAX_LENGTH_STRING);
                    }
                });
    }

    @Table("ENTITY")
    public static interface LargeTextColumn extends RawEntity<Integer>
    {
        @NotNull
        @PrimaryKey("ID")
        public int getID();

        void setId(int id);

        @StringLength (TypeQualifiers.OLD_MAX_STRING_LENGTH)
        String getText();

        void setText(String text);
    }

}
