package net.java.ao;

import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;
import net.java.ao.db.SQLServerDatabaseProvider;

public class DatabaseProviders
{
    public static HSQLDatabaseProvider getHsqlDatabaseProvider(DisposableDataSource source)
    {
        return new HSQLDatabaseProvider(source);
    }

    public static PostgreSQLDatabaseProvider getPostgreSqlDatabaseProvider(DisposableDataSource source)
    {
        return new PostgreSQLDatabaseProvider(source);
    }

    public static OracleDatabaseProvider getOrableDatabaseProvider(DisposableDataSource source)
    {
        return new OracleDatabaseProvider(source);
    }

    public static MySQLDatabaseProvider getMySqlDatabaseProvider(DisposableDataSource source)
    {
        return new MySQLDatabaseProvider(source);
    }

    public static SQLServerDatabaseProvider getMsSqlDatabaseProvider(DisposableDataSource source)
    {
        return new SQLServerDatabaseProvider(source);
    }

    public static EmbeddedDerbyDatabaseProvider getEmbeddedDerbyDatabaseProvider(DisposableDataSource source)
    {
        return new EmbeddedDerbyDatabaseProvider(source, "");
    }
}
