package net.java.ao.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLValue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class DdlActionAssert {
    private DdlActionAssert() {
    }

    public static void assertIsDropTables(Iterator<DDLAction> actions, boolean caseSensitive, String... expectedTableNames) {
        final Collection<String> names = Lists.newArrayList(expectedTableNames);
        while (!names.isEmpty()) {
            assertIsDropTable(actions.next(), caseSensitive, names);
        }
    }

    public static void assertIsCreateTables(Iterator<DDLAction> actions, boolean caseSensitive, String... expectedTableNames) {
        final Collection<String> names = Lists.newArrayList(expectedTableNames);
        while (!names.isEmpty()) {
            assertIsCreateTable(actions.next(), caseSensitive, names);
        }
    }

    private static void assertIsDropTable(final DDLAction action, final boolean caseSensitive, Collection<String> names) {
        assertEquals(DDLActionType.DROP, action.getActionType());
        find(action.getTable().getName(), names, caseSensitive);
    }

    private static void assertIsCreateTable(final DDLAction action, final boolean caseSensitive, Collection<String> names) {
        assertEquals(DDLActionType.CREATE, action.getActionType());
        find(action.getTable().getName(), names, caseSensitive);
    }

    private static void find(String name, Collection<String> names, boolean caseSensitive) {
        final String expectedTableName = Iterables.find(names, new StringEqualsPredicate(name, caseSensitive));
        assertNotNull(expectedTableName);
        names.remove(expectedTableName);
    }

    public static void assertIsDropForeignKey(DDLAction action, String fromTable, String toTable, boolean caseSensitive) {
        assertEquals(DDLActionType.ALTER_DROP_KEY, action.getActionType());
        assertEqualsAccordingToCaseSensitivity(fromTable, action.getKey().getDomesticTable(), caseSensitive);
        assertEqualsAccordingToCaseSensitivity(toTable, action.getKey().getTable(), caseSensitive);
    }

    public static void assertIsCreateForeignKey(DDLAction action, String fromTable, String toTable, boolean caseSensitive) {
        assertEquals(DDLActionType.ALTER_ADD_KEY, action.getActionType());
        assertEqualsAccordingToCaseSensitivity(fromTable, action.getKey().getDomesticTable(), caseSensitive);
        assertEqualsAccordingToCaseSensitivity(toTable, action.getKey().getTable(), caseSensitive);
    }

    public static void assertIsInsert(DDLAction action, boolean caseSensitive, String tableName, Map<String, Object> values) {
        assertEquals(DDLActionType.INSERT, action.getActionType());
        assertEqualsAccordingToCaseSensitivity(tableName, action.getTable().getName(), caseSensitive);

        final Map<String, Object> copy = Maps.newHashMap(values);
        for (DDLValue value : action.getValues()) {
            final String fieldName = value.getField().getName();
            final Map<String, Object> filtered = Maps.filterKeys(copy, new StringEqualsPredicate(fieldName, caseSensitive));
            assertEquals("There should be one and only one entry for key " + fieldName
                            + ". Here are the actual values: " + valuesAsString(action.getValues())
                            + " and the expected ones: " + copy,
                    1, filtered.size());
            Map.Entry<String, Object> entry = filtered.entrySet().iterator().next();

            if (entry.getValue() instanceof Number) {
                assertEquals("Wrong value for field " + fieldName, ((Number) entry.getValue()).doubleValue(), ((Number) value.getValue()).doubleValue(), 0);
            } else {
                assertEquals("Wrong value for field " + fieldName, entry.getValue(), value.getValue());
            }

            copy.remove(entry.getKey());
        }
        assertTrue("Not all expected values were defined", copy.isEmpty());
    }

    private static String valuesAsString(DDLValue[] values) {
        final StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (DDLValue value : values) {
            sb.append(value.getField().getName()).append(":").append(value.getValue()).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.append("]").toString();
    }

    public static void assertActionsEquals(Iterable<DDLAction> expected, Iterable<DDLAction> actual) {
        assertActionsEquals(expected.iterator(), actual.iterator());
    }

    private static void assertActionsEquals(Iterator<DDLAction> expected, Iterator<DDLAction> actual) {
        int index = 0;
        while (expected.hasNext() || actual.hasNext()) {
            if (!expected.hasNext()) {
                fail("Actual collections of action has more elements than expected, current index is " + index);
            }
            if (!actual.hasNext()) {
                fail("Expected more actions than actually available, current index is " + index);
            }
            final DDLAction expectedAction = expected.next();
            final DDLAction actualAction = actual.next();
            assertEquals("Actions at index " + index + " do not match!", expectedAction, actualAction);
            index++;
        }
    }

    private static String fieldName(DDLField field) {
        return field.getName();
    }

    private static String tableName(DDLAction action) {
        return action.getTable().getName();
    }

    private static void assertEqualsAccordingToCaseSensitivity(String expected, String actual, boolean caseSensitive) {
        if (!caseSensitive) {
            assertTrue("Expected <" + expected + "> equals <" + actual + "> ignoring the case.",
                    expected.equalsIgnoreCase(actual));
        } else {
            assertTrue("Expected <" + expected + "> equals <" + actual + "> respecting the case.",
                    expected.equals(actual));
        }
    }

    private static class StringEqualsPredicate implements Predicate<String> {
        private final String string;
        private final boolean caseSensitive;

        public StringEqualsPredicate(String string, boolean caseSensitive) {
            this.string = string;
            this.caseSensitive = caseSensitive;
        }

        public boolean apply(String name) {
            if (caseSensitive) {
                return string.equals(name);
            } else {
                return string.equalsIgnoreCase(name);
            }
        }
    }
}
