package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.getHsqlDatabaseProvider;

public final class HsqlDatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "hsql";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getHsqlDatabaseProvider();
    }
}
