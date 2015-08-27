package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.getNuoDBDatabaseProvider;

public final class NuoDBDatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "nuodb";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getNuoDBDatabaseProvider();
    }
}
