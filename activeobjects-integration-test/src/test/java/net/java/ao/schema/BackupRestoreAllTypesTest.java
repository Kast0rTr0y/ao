package net.java.ao.schema;

import net.java.ao.RawEntity;
import net.java.ao.it.model.backup.EntityWithBooleanType;
import net.java.ao.it.model.backup.EntityWithStringType;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import static net.java.ao.schema.DdlActionAssert.*;
import static org.junit.Assert.assertEquals;

public final class BackupRestoreAllTypesTest extends AbstractBackupRestoreTest
{
    @Test
    @NonTransactional
    public void backupRestoreTableWithVarcharField() throws Exception
    {
        backupTableWithField(EntityWithStringType.class, "string", Types.VARCHAR, 301);
    }

    @Test
    @NonTransactional
    public void backupRestoreTableWithBooleanField() throws Exception
    {
        backupTableWithField(EntityWithBooleanType.class, "condition", Types.BOOLEAN, 1);
    }

    private void backupTableWithField(Class<? extends RawEntity<?>> entityClass, String fieldName, int sqlType, int precision) throws Exception
    {
        final boolean caseSensitive = getProvider().isCaseSensetive();

        entityManager.migrate(entityClass);

        final List<DDLAction> backup = backup();
        final Iterator<DDLAction> actions = backup.iterator();

        assertIsDropTable(getTableName(entityClass), actions.next(), caseSensitive);

        final DDLAction createTableAction = actions.next();
        assertIsCreateTable(getTableName(entityClass), createTableAction, caseSensitive);
        assertFieldEquals(fieldName, sqlType, precision, getField(createTableAction), caseSensitive);

        emptyDatabase();

        restore(backup);
        assertEquals(backup, backup()); // make sure backup are the same
    }

    /**
     * Gets the field of the table associated to the action, assuming that this table exists and
     * has one and only one field.
     *
     * @param action the action to look up the field for
     * @return a field
     */
    private DDLField getField(DDLAction action)
    {
        return action.getTable().getFields()[0];
    }
}
