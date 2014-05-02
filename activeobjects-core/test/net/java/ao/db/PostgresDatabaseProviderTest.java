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

    @Override
    protected String getExpectedWhereClauseWithWildcard()
    {
        // this is invalid SQL but is used to check that field1 is potentially quoted but * isn't
        // PostgreSQL should quote all but wildcards and digits
        return "\"field1\" = *";
    }

    @Override
    protected String getExpectedWhereClauseWithUnderscore()
    {
        return "\"_field1\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithNumericIdentifier()
    {
        // PostgreSQL should quote all but wildcards and digits
        return "\"12345abc\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithUnderscoredNumeric()
    {
        // PostgreSQL should quote all but wildcards and digits
        return "\"_12345abc\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithAlphaNumeric()
    {
        // PostgreSQL should quote all but wildcards and digits
        return "\"a12345bc\" = 1";
    }

    @Override
    protected String[] getExpectedOrderClauses() {
        return new String[] {
                "\"column1\"",
                "\"column1\" ASC",
                "\"column1\" DESC",
                "table1.\"column1\"",
                "table1.\"column1\" ASC",
                "table1.\"column1\" ASC, \"column2\"",
                "\"column1\", table2.\"column2\" ASC",
                "table1.\"column1\" ASC, table2.\"column2\" ASC"
        };
    }
}
