package net.java.ao.builder;

import net.java.ao.ActiveObjectsException;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.db.ClientDerbyDatabaseProvider;
import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.H2DatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.MsJdbcSQLServerDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;
import net.java.ao.db.SQLServerDatabaseProvider;

import java.sql.Driver;

import static com.google.common.base.Preconditions.checkNotNull;

enum SupportedDatabase {
    // Note: The order IS important!
    MYSQL("jdbc:mysql", "com.mysql.jdbc.Driver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new MySQLDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password));
        }
    },
    // Note: MariaDB's driver can also handle jdbc:mysql URLs, but those will already return the correct provider via
    // the MYSQL enum entry above so they are not handled separately here (since each urlPrefix must be unique)
    MARIA_DB("jdbc:mariadb", "org.mariadb.jdbc.Driver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new MySQLDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password));
        }
    },
    DERBY_NETWORK("jdbc:derby://", "org.apache.derby.jdbc.ClientDriver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new ClientDerbyDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password));
        }
    },
    DERBY_EMBEDDED("jdbc:derby", "org.apache.derby.jdbc.EmbeddedDriver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new EmbeddedDerbyDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), uri);
        }
    },
    ORACLE("jdbc:oracle", "oracle.jdbc.OracleDriver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new OracleDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    },
    POSTGRESQL("jdbc:postgresql", "org.postgresql.Driver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new PostgreSQLDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    },
    MSSQL("jdbc:sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new MsJdbcSQLServerDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    },
    MSSQL_JTDS("jdbc:jtds:sqlserver", "net.sourceforge.jtds.jdbc.Driver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new SQLServerDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    },
    HSQLDB("jdbc:hsqldb", "org.hsqldb.jdbcDriver") {
        @Override
        public DatabaseProvider getDatabaseProvider(DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new HSQLDatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    },
    H2_EMBEDDED("jdbc:h2", "org.h2.Driver") {
        @Override
        public DatabaseProvider getDatabaseProvider(final DataSourceFactory dataSourceFactory, String uri, String username, String password, String schema) {
            return new H2DatabaseProvider(getDataSource(dataSourceFactory, uri, username, password), schema);
        }
    };

    private final String uriPrefix;
    private final String driverClassName;

    SupportedDatabase(String uriPrefix, String driverClassName) {
        this.uriPrefix = checkNotNull(uriPrefix);
        this.driverClassName = checkNotNull(driverClassName);
    }

    public abstract DatabaseProvider getDatabaseProvider(DataSourceFactory factory, String url, String username, String password, String schema);

    DisposableDataSource getDataSource(DataSourceFactory factory, String uri, String username, String password) {
        final Class<? extends Driver> driverClass = checkDriverLoaded();
        return factory.getDataSource(driverClass, uri, username, password);
    }

    /**
     * Checks whether the URI starts with the prefix associated with the database
     *
     * @param uri the give URI for connecting to the database
     * @return {@code true} if the URI is valid for this instance of data source factory
     */
    private boolean accept(String uri) {
        return checkNotNull(uri).trim().startsWith(uriPrefix);
    }

    private Class<? extends Driver> checkDriverLoaded() {
        try {
            return getDriverClass(driverClassName);
        } catch (ClassNotFoundException e) {
            throw new UnloadableJdbcDriverException(driverClassName, e);
        }
    }

    @Override
    public String toString() {
        return "Database with prefix " + uriPrefix + " and driver " + driverClassName;
    }

    static SupportedDatabase fromUri(String uri) {
        for (SupportedDatabase supported : values()) {
            if (supported.accept(uri)) {
                return supported;
            }
        }
        throw new ActiveObjectsException("Could not resolve database for database connection URI <" + uri + ">, are you sure this database is supported");
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Driver> getDriverClass(String driverClassName) throws ClassNotFoundException {
        return (Class<? extends Driver>) Class.forName(driverClassName);
    }
}
