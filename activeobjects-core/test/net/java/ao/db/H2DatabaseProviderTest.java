package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.getH2DatabaseProvider;

public class H2DatabaseProviderTest extends DatabaseProviderTest {
    @Override
    protected String getDatabase() {
        return "h2";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getH2DatabaseProvider();
    }
}
