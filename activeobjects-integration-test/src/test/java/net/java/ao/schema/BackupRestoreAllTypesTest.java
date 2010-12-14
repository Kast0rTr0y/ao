package net.java.ao.schema;

import com.google.common.collect.Iterators;
import net.java.ao.RawEntity;
import net.java.ao.it.model.backup.EntityWithBooleanType;
import net.java.ao.it.model.backup.EntityWithStringType;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.sql.Types;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
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
        final HashSet<Integer> sqlTypes = newHashSet(Types.BOOLEAN, Types.BIT, Types.NUMERIC); // MySQL uses BIT for boolean type, Oracle uses NUMERIC
        backupTableWithField(EntityWithBooleanType.class, "condition", sqlTypes, 1);
    }

    private void backupTableWithField(Class<? extends RawEntity<?>> entityClass, String fieldName, int sqlType, int precision) throws Exception
    {
        backupTableWithField(entityClass, fieldName, singleton(sqlType), precision);
    }

    private void backupTableWithField(Class<? extends RawEntity<?>> entityClass, String fieldName, Set<Integer> sqlTypes, int precision) throws Exception
    {
        entityManager.migrate(entityClass);

        final List<DDLAction> backup = backup();
        final Iterator<DDLAction> actions = backup.iterator();

        assertIsDropTables(actions, isCaseSensitive(), getTableName(entityClass));

        final DDLAction createTableAction = actions.next();
        assertIsCreateTables(Iterators.forArray(createTableAction), isCaseSensitive(), getTableName(entityClass));
        assertFieldEquals(fieldName, sqlTypes, precision, getField(createTableAction), isCaseSensitive());

        emptyDatabase();

        restore(backup);
        assertEquals(backup, backup()); // make sure backup are the same
    }

    /**
     * Gets the field of the table associated to the action, assuming that this table exists and
     * has one and only one field (apart from the ID).
     *
     * @param action the action to look up the field for
     * @return a field
     */
    private DDLField getField(DDLAction action)
    {
        final DDLField[] fields = action.getTable().getFields();
        assertEquals(2, fields.length);
        return fields[0].getName().equalsIgnoreCase("ID") ? fields[1] : fields[0];
    }
}
