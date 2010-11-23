package net.java.ao.schema;

import com.google.common.collect.ImmutableMap;
import net.java.ao.EntityManager;
import net.java.ao.it.model.backup.EntityWithManyToOne;
import net.java.ao.it.model.backup.EntityWithName;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static net.java.ao.schema.DdlActionAssert.*;

@Data(BackupRestoreTablesWithConstraintsTest.BackupRestoreTablesWithConstraintsTestDatabaseUpdater.class)
public final class BackupRestoreTablesWithConstraintsTest extends AbstractBackupRestoreTest
{
    private static int ENTITY_WITH_NAME_ID;
    private static int ENTITY_WITH_MANY_TO_ONE_ID;

    @Test
    @NonTransactional
    public void tablesWithForeignKeyConstraints() throws Exception
    {
        final String entityWithNameTableName = getTableName(EntityWithName.class);
        final String entityWithManyToOneTableName = getTableName(EntityWithManyToOne.class);

        final List<DDLAction> backup = backup();
        final Iterator<DDLAction> actions = backup.iterator();

        assertIsDropForeignKey(actions.next(), entityWithManyToOneTableName, entityWithNameTableName, isCaseSensitive());
        assertIsDropTables(actions, isCaseSensitive(), entityWithNameTableName, entityWithManyToOneTableName);
        assertIsCreateTables(actions, isCaseSensitive(), entityWithNameTableName, entityWithManyToOneTableName);

        assertIsInsert(actions.next(), isCaseSensitive(), entityWithManyToOneTableName,
                ImmutableMap.<String, Object>of("id", ENTITY_WITH_MANY_TO_ONE_ID, "entityWithNameId", ENTITY_WITH_NAME_ID));

        assertIsInsert(actions.next(), isCaseSensitive(), entityWithNameTableName,
                ImmutableMap.<String, Object>of("id", ENTITY_WITH_NAME_ID, "name", BackupRestoreTablesWithConstraintsTestDatabaseUpdater.NAME));

        assertIsCreateForeignKey(actions.next(), entityWithManyToOneTableName, entityWithNameTableName, isCaseSensitive());

        emptyDatabase();
        restore(backup);

        assertActionsEquals(backup, backup());
    }

    public static class BackupRestoreTablesWithConstraintsTestDatabaseUpdater implements DatabaseUpdater
    {
        static final String NAME = "an named entity";

        public void update(EntityManager entityManager) throws Exception
        {
            entityManager.migrate(EntityWithManyToOne.class, EntityWithName.class);

            final EntityWithName entityWithName = entityManager.create(EntityWithName.class);
            entityWithName.setName(NAME);
            entityWithName.save();

            ENTITY_WITH_NAME_ID = entityWithName.getID();

            final EntityWithManyToOne entityWithManyToOne = entityManager.create(EntityWithManyToOne.class);
            entityWithManyToOne.setEntityWithName(entityWithName);
            entityWithManyToOne.save();

            ENTITY_WITH_MANY_TO_ONE_ID = entityWithManyToOne.getID();
        }
    }
}
