package net.java.ao.schema.ddl;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class DDLActionsTest {
    @Test
    public void testNewAlterAddKey() throws Exception {
        final DDLForeignKey key = mock(DDLForeignKey.class);
        final DDLAction action = DDLActions.newAlterAddKey(key);

        assertEquals(DDLActionType.ALTER_ADD_KEY, action.getActionType());
        assertSame(key, action.getKey());
    }

    @Test
    public void testNewInsert() throws Exception {
        final DDLTable table = mock(DDLTable.class);
        final DDLValue[] values = new DDLValue[]{};
        final DDLAction action = DDLActions.newInsert(table, values);

        assertSame(table, action.getTable());
        assertSame(values, action.getValues());
    }
}
