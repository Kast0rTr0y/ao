package net.java.ao.it.datatypes;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.types.TypeQualifiers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    public void testmigration() throws Exception
    {
        entityManager.migrate(VarcharColumn.class);

        final VarcharColumn e = entityManager.create(VarcharColumn.class);
        e.setText("Fred");
        e.save();

        entityManager.migrate(LargeTextColumn.class);
        entityManager.flushAll();

        LargeTextColumn retrieved = entityManager.get(LargeTextColumn.class, e.getID());
        assertEquals("Fred", retrieved.getText());

        retrieved.setText(LARGE_STRING);
        retrieved.save();
    }

    @Table("ENTITY")
    public static interface LargeTextColumn extends Entity
    {
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
