package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.getMySqlDatabaseProvider;

public final class MySqlDatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "mysql";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getMySqlDatabaseProvider();
    }
}
