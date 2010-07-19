package net.java.ao;

import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.JTDSSQLServerDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;

import javax.sql.DataSource;

public class DatabaseProviders
{
    public static HSQLDatabaseProvider getHsqlDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new HSQLDatabaseProvider(Database.HSQLDB, source);
    }

    public static PostgreSQLDatabaseProvider getPostgreSqlDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new PostgreSQLDatabaseProvider(Database.POSTGRES, source);
    }

    public static OracleDatabaseProvider getOrableDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new OracleDatabaseProvider(Database.ORACLE, source);
    }

    public static MySQLDatabaseProvider getMySqlDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new MySQLDatabaseProvider(Database.MYSQL, source);
    }

    public static JTDSSQLServerDatabaseProvider getJtdsMsSqlDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new JTDSSQLServerDatabaseProvider(Database.MSSQL, source);
    }

    public static EmbeddedDerbyDatabaseProvider getEmbeddedDerbyDatabaseProvider(ActiveObjectsDataSource source)
    {
        return new EmbeddedDerbyDatabaseProvider(Database.DERBY, source);
    }
}
