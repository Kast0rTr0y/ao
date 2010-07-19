package net.java.ao.builder;

import net.java.ao.ActiveObjectsDataSource;
import net.java.ao.ActiveObjectsException;
import net.java.ao.Database;
import net.java.ao.DatabaseProvider;
import net.java.ao.db.ClientDerbyDatabaseProvider;
import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.JTDSSQLServerDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;
import net.java.ao.db.SQLServerDatabaseProvider;

import javax.sql.DataSource;

import java.sql.Driver;

import static com.google.common.base.Preconditions.checkNotNull;

enum SupportedDatabase
{
    // Note: the order IS important!
    MYSQL("jdbc:mysql", "com.mysql.jdbc.Driver", Database.MYSQL)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new MySQLDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    DERBY_NETWORK("jdbc:derby://", "org.apache.derby.jdbc.ClientDriver", Database.DERBY)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new ClientDerbyDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    DERBY_EMBEDDED("jdbc:derby", "org.apache.derby.jdbc.EmbeddedDriver", Database.DERBY)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new EmbeddedDerbyDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    ORACLE("jdbc:oracle", "oracle.jdbc.OracleDriver", Database.ORACLE)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new OracleDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    POSTGRESQL("jdbc:postgresql", "org.postgresql.Driver", Database.POSTGRES)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new PostgreSQLDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    MSSQL("jdbc:sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", Database.MSSQL)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new SQLServerDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    MSSQL_JTDS("jdbc:jtds:sqlserver", "net.sourceforge.jtds.jdbc.Driver", Database.MSSQL)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new JTDSSQLServerDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            },
    HSQLDB("jdbc:hsqldb", "org.hsqldb.jdbcDriver", Database.HSQLDB)
            {
                @Override
                public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password)
                {
                    return new HSQLDatabaseProvider(getDatabase(), getDataSource(dataSourceFactory, uri, username, password));
                }
            };

    private final String uriPrefix;
    private final String driverClassName;
    private final Database database;

    SupportedDatabase(String uriPrefix, String driverClassName, Database database)
    {
        this.uriPrefix = checkNotNull(uriPrefix);
        this.driverClassName = checkNotNull(driverClassName);
        this.database = checkNotNull(database);
    }

    public abstract DatabaseProvider getDatabaseProvider(DataSourceFactory factory, String url, String username, String password);

    Database getDatabase()
    {
        return database;
    }

    ActiveObjectsDataSource getDataSource(DataSourceFactory factory, String uri, String username, String password)
    {
        final Class<? extends Driver> driverClass = checkDriverLoaded();
        return factory.getDataSource(driverClass, uri, username, password);
    }

    /**
     * Checks whether the URI starts with the prefix assocoated with the database
     *
     * @param uri the give URI for connecting to the database
     * @return {@code true} if the URI is valid for this instance of data source factory
     */
    private boolean accept(String uri)
    {
        return checkNotNull(uri).trim().startsWith(uriPrefix);
    }

    private Class<? extends Driver> checkDriverLoaded()
    {
        try
        {
            return (Class<? extends Driver>) Class.forName(driverClassName);
        }
        catch (ClassNotFoundException e)
        {
            throw new UnloadableJdbcDriverException(driverClassName, e);
        }
    }

    static SupportedDatabase getFromUri(String uri)
    {
        for (SupportedDatabase supported : values())
        {
            if (supported.accept(uri))
            {
                return supported;
            }
        }
        throw new ActiveObjectsException("Could not resolve database for database connection URI <" + uri + ">, are you sure this database is supported");
    }

    @Override
    public String toString()
    {
        return new StringBuilder().append("Database ").append(database).append(" with driver ").append(driverClassName).toString();
    }
}
