package net.java.ao.schema;

import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;

import static org.junit.Assert.*;

public final class DdlActionAssert
{
    private DdlActionAssert()
    {
    }

    public static void assertIsDropTable(String expectedTableName, DDLAction action, boolean caseSensitive)
    {
        assertEquals(DDLActionType.DROP, action.getActionType());
        assertTableNameEquals(expectedTableName, action, caseSensitive);
    }

    public static void assertIsCreateTable(String expectedTableName, DDLAction action, boolean caseSensitive)
    {
        assertEquals(DDLActionType.CREATE, action.getActionType());
        assertTableNameEquals(expectedTableName, action, caseSensitive);
    }

    private static void assertTableNameEquals(String expectedTableName, DDLAction action, boolean caseSensitive)
    {
        assertEqualsAccordingToCaseSensitivity(expectedTableName, tableName(action), caseSensitive);
    }

    public static void assertFieldEquals(String expectedName, int expectedType, int expectedPrecision, DDLField field, boolean caseSensitive)
    {
        assertEqualsAccordingToCaseSensitivity(expectedName, fieldName(field), caseSensitive);
        assertEquals(expectedType, field.getType().getType());
        assertEquals(expectedPrecision, field.getPrecision());
    }

    private static String fieldName(DDLField field)
    {
        return field.getName();
    }

    private static String tableName(DDLAction action)
    {
        return action.getTable().getName();
    }

    private static void assertEqualsAccordingToCaseSensitivity(String expected, String actual, boolean caseSensitive)
    {
        if (!caseSensitive)
        {
            assertTrue("Expected <" + expected + "> equals <" + actual + "> ignoring the case.",
                    expected.equalsIgnoreCase(actual));
        }
        else
        {
            assertTrue("Expected <" + expected + "> equals <" + actual + "> respecting the case.",
                    expected.equals(actual));
        }
    }
}
