package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.*;

public final class OracleDatabaseProviderTest extends DatabaseProviderTest
{
    @Override
    protected String getDatabase()
    {
        return "oracle";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return getOracleDatabaseProvider();
    }
}
