package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.getMsSqlDatabaseProvider;

public final class SqlServerDatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "sqlserver";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getMsSqlDatabaseProvider();
    }
}
