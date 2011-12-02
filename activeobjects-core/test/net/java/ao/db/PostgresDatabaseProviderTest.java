package net.java.ao.db;

import net.java.ao.DatabaseProvider;

import static net.java.ao.DatabaseProviders.*;

public final class PostgresDatabaseProviderTest extends DatabaseProviderTest
{
    @Override
    protected String getDatabase()
    {
        return "postgres";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return getPostgreSqlDatabaseProvider();
    }

    @Override
    protected String getExpectedWhereClause()
    {
        return "\"field1\" = 2 and \"field2\" like %er";
    }
}
