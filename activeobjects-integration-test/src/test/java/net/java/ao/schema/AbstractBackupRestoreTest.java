package net.java.ao.schema;

import net.java.ao.DatabaseProvider;
import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.it.config.DynamicJdbcConfiguration;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

import static net.java.ao.sql.SqlUtils.*;
import static org.junit.Assert.*;

@Jdbc(DynamicJdbcConfiguration.class)
@Data
@RunWith(ActiveObjectsJUnitRunner.class)
public abstract class AbstractBackupRestoreTest
{
    private BackupRestore backupRestore;

    protected EntityManager entityManager;

    @Before
    public final void configureBackupRestore() throws Exception
    {
        backupRestore = new BackupRestoreImpl();
    }

    protected final void restore(List<DDLAction> backup)
    {
        backupRestore.restore(backup, getProvider());
    }

    protected final List<DDLAction> backup()
    {
        return backupRestore.backup(getProvider(), new DefaultSchemaConfiguration());
    }

    protected final String getTableNameIgnoreCase(String originalTableName)
    {
        return getProvider().isCaseSensetive() ? originalTableName : originalTableName.toLowerCase();
    }

    protected final String getTableName(Class<? extends RawEntity<?>> entityClass)
    {
        return getTableNameConverter().getName(entityClass);
    }

    protected final TableNameConverter getTableNameConverter()
    {
        return entityManager.getTableNameConverter();
    }

    protected final void emptyDatabase() throws Exception
    {
        entityManager.migrate(); // removes all data from the database
        entityManager.flushAll();
        assertDatabaseIsEmpty();
    }

    protected final void assertDatabaseIsNotEmpty() throws Exception
    {
        assertDatabaseIsEmpty(false);
    }

    private void assertDatabaseIsEmpty() throws Exception
    {
        assertDatabaseIsEmpty(true);
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

    private DatabaseProvider getProvider()
    {
        return entityManager.getProvider();
    }

    protected final boolean isCaseSensitive()
    {
        return getProvider().isCaseSensetive();
    }
}
