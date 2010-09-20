package net.java.ao.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import net.java.ao.DatabaseProvider;
import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.it.model.backup.Animal;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Hsql;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.size;
import static net.java.ao.schema.BackupRestoreDatabaseUpdater.AnimalData;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static org.junit.Assert.assertEquals;

/**
 *
 */
@Data(BackupRestoreDatabaseUpdater.class)
@Jdbc(Hsql.class)
@RunWith(ActiveObjectsJUnitRunner.class)
public class BackupRestoreImplTest
{
    private BackupRestore backupRestore;

    private EntityManager entityManager;
    private DefaultSchemaConfiguration schemaConfiguration;

    @Before
    public void setUp() throws Exception
    {
        backupRestore = new BackupRestoreImpl();
        schemaConfiguration = new DefaultSchemaConfiguration();
    }

    @After
    public void tearDown() throws Exception
    {
        schemaConfiguration = null;
        backupRestore = null;
    }

    @Test
    @NonTransactional
    public void testBackupAndRestore() throws Exception
    {
        final List<DDLAction> backup = backupRestore.backup(getProvider(), schemaConfiguration);

        assertEquals(5, backup.size());
        final Iterator<DDLAction> backupIt = backup.iterator();
        assertIsDropTable(Animal.class, backupIt.next());
        assertIsCreateTable(Animal.class, backupIt.next());
        assertIsInsert(Animal.class, backupIt.next());
        assertIsInsert(Animal.class, backupIt.next());
        assertIsInsert(Animal.class, backupIt.next());

        entityManager.migrate(); // removes all data from the database
        entityManager.flushAll();

        assertDatabaseIsEmpty();

        backupRestore.restore(backup, getProvider());

        assertDatabaseIsNotEmpty();

        final Collection<Animal> animals = Lists.newArrayList(entityManager.find(Animal.class));
        for (final AnimalData animalData : AnimalData.ALL)
        {
            assertEquals("Should have found one and only one of " + animalData + " |", 1, size(filter(animals, new Predicate<Animal>()
            {
                public boolean apply(Animal animal)
                {
                    return animal.getName().equals(animalData.name);
                }
            })));
        }
    }

    private void assertDatabaseIsEmpty() throws Exception
    {
        assertDatabaseIsEmpty(true);
    }

    private void assertDatabaseIsNotEmpty() throws Exception
    {
        assertDatabaseIsEmpty(false);
    }

    private void assertDatabaseIsEmpty(boolean isEmpty) throws Exception
    {
        Connection connection = null;
        ResultSet tables = null;
        try
        {
            connection = getProvider().getConnection();
            tables = getProvider().getTables(connection);
            assertEquals(!isEmpty, tables.next());
        }
        finally
        {
            closeQuietly(tables);
            closeQuietly(connection);
        }
    }

    private void assertIsDropTable(Class<? extends RawEntity<?>> entityClass, DDLAction ddlAction)
    {
        assertEquals(DDLActionType.DROP, ddlAction.getActionType());
        assertTableNameEquals(entityClass, ddlAction.getTable());
    }

    private void assertIsCreateTable(Class<? extends RawEntity<?>> entityClass, DDLAction ddlAction)
    {
        assertEquals(DDLActionType.CREATE, ddlAction.getActionType());
        assertTableNameEquals(entityClass, ddlAction.getTable());
    }

    private void assertIsInsert(Class<? extends RawEntity<?>> entityClass, DDLAction ddlAction)
    {
        assertEquals(DDLActionType.INSERT, ddlAction.getActionType());
        assertTableNameEquals(entityClass, ddlAction.getTable());
    }

    private void assertTableNameEquals(Class<? extends RawEntity<?>> entityClass, DDLTable ddlTable)
    {
        if (getProvider().isCaseSensetive())
        {
            assertEquals(getTableName(entityClass), ddlTable.getName());
        }
        else
        {
            assertEquals(getTableName(entityClass).toLowerCase(), ddlTable.getName().toLowerCase());
        }
    }

    private String getTableName(Class<? extends RawEntity<?>> entityClass)
    {
        return getTableNameConverter().getName(entityClass);
    }

    private TableNameConverter getTableNameConverter()
    {
        return entityManager.getTableNameConverter();
    }

    private DatabaseProvider getProvider()
    {
        return entityManager.getProvider();
    }
}
