package net.java.ao.schema;

import net.java.ao.DatabaseProvider;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLAction;

import java.util.List;

/**
 * <p>Allows backing up and restoring to a database "defined" by the given {@link net.java.ao.DatabaseProvider}.</p>
 * <p>calling {@code restore(backup(provider,configuration) provider)} with the {@link net.java.ao.DefaultSchemaConfiguration}
 * should be idempotent.</p>
 */
public interface BackupRestore
{
    /**
     * Backs up the database into a list of {@link net.java.ao.schema.ddl.DDLAction}
     *
     * @param provider the database provider
     * @param configuration the schema configuration
     * @return a list of {@link net.java.ao.schema.ddl.DDLAction} representing the backed up database
     */
    List<DDLAction> backup(DatabaseProvider provider, SchemaConfiguration configuration);

    /**
     * Restores a list of of {@link net.java.ao.schema.ddl.DDLAction} in the give database
     *
     * @param actions the {@link net.java.ao.schema.ddl.DDLAction DDL actions} to execute.
     * @param provider the provider to the database to restore
     */
    void restore(List<DDLAction> actions, DatabaseProvider provider);
}
