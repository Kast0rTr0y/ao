package net.java.ao.schema.ddl;

import static net.java.ao.schema.ddl.DDLActionType.ALTER_ADD_KEY;
import static net.java.ao.schema.ddl.DDLActionType.INSERT;

/**
 * A utility class to manipulate {@link net.java.ao.schema.ddl.DDLAction DDL Actions}.
 * Do not instantiate.
 */
public final class DDLActions {
    private DDLActions() {
    }

    public static DDLAction newAlterAddKey(DDLForeignKey key) {
        final DDLAction action = newAction(ALTER_ADD_KEY);
        action.setKey(key);
        return action;
    }

    public static DDLAction newInsert(DDLTable table, DDLValue[] values) {
        final DDLAction action = newAction(INSERT);
        action.setTable(table);
        action.setValues(values);
        return action;
    }

    private static DDLAction newAction(DDLActionType actionType) {
        return new DDLAction(actionType);
    }
}
