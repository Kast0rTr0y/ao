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

public final class MigrationFromUnlimitedToStringTest extends ActiveObjectsIntegrationTest
{

    private static String LARGE_STRING;
    private static final String MAX_LENGTH_STRING;
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
        MAX_LENGTH_STRING = sb.substring(0, StringLength.MAX_LENGTH);
    }

    @Test
    @NonTransactional
    public void testMigration() throws Exception
    {
        entityManager.migrate(LargeTextColumn.class);

        final VarcharColumn e = entityManager.create(VarcharColumn.class,
                new DBParam("ID", 1));
        e.setText(LARGE_STRING);
        e.save();

        entityManager.migrate(VarcharColumn.class);

        VarcharColumn retrieved = entityManager.get(VarcharColumn.class, e.getID());
        assertEquals(MAX_LENGTH_STRING, retrieved.getText());
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
    public static interface VarcharColumn extends RawEntity<Integer>
    {
        @NotNull
        @PrimaryKey("ID")
        public int getID();

        void setId(int id);

        @StringLength(StringLength.MAX_LENGTH)
        public String getText();

        public void setText(String text);
    }

}
