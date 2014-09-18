package net.java.ao.it.datatypes;

import java.sql.PreparedStatement;
import net.java.ao.DBParam;
import net.java.ao.DatabaseProvider;
import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class MigrationFromStringToUnlimitedTest extends ActiveObjectsIntegrationTest
{

    private static String LARGE_STRING;
    static
    {
        String s = "123456789#"; // 10 chars
        StringBuilder sb = new StringBuilder(s.length() * 600);
        for (int i = 0; i < 600; i++)
        {
            sb.append(s);
        }
        sb.append(sb.length() + 4);
        LARGE_STRING = sb.toString();
    }
    @Test
    @NonTransactional
    public void testMigration() throws Exception
    {
        entityManager.migrate(VarcharColumn.class);

        final VarcharColumn e = entityManager.create(VarcharColumn.class);
        e.setText("Fred");
        e.save();

        entityManager.migrate(LargeTextColumn.class);

        LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, e.getID());
        assertEquals("Fred", retrieved.getText());

        retrieved.setText(LARGE_STRING);
        retrieved.save();
    }

    @Test
    @NonTransactional
    public void testMigrationFromOversizedColumn() throws Exception
    {
        DatabaseProvider provider  = entityManager.getProvider();
        DbUtils.executeUpdate(entityManager,
                "CREATE TABLE " + getTableName(VarcharColumn.class) + "(" + provider.processID("ID")+" INTEGER PRIMARY KEY, " + provider.processID("TEXT") + " VARCHAR(767))",
                new DbUtils.UpdateCallback()
                {
                    @Override
                    public void setParameters(final PreparedStatement statement) throws Exception
                    {

                    }
                });
        entityManager.migrate(LargeTextColumn.class);
        final LargeTextColumn e = entityManager.create(LargeTextColumn.class,
                new DBParam("ID", 1), new DBParam("TEXT",LARGE_STRING));
        final LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, 1);

    }

    @Table("ENTITY")
    public static interface LargeTextColumn extends RawEntity<Integer>
    {
        @NotNull
        @PrimaryKey("ID")
        public int getID();

        void setId(int id);

        @StringLength (StringLength.UNLIMITED)
        String getText();

        void setText(String text);
    }

    @Table("ENTITY")
    public static interface VarcharColumn extends Entity
    {
        @StringLength(StringLength.MAX_LENGTH)
        public String getText();

        public void setText(String text);
    }

}
