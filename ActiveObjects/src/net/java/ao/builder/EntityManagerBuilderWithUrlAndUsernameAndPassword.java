package net.java.ao.builder;

import net.java.ao.ActiveObjectsException;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityManagerBuilderWithUrlAndUsernameAndPassword
{
    private final String url, username, password;

    public EntityManagerBuilderWithUrlAndUsernameAndPassword(String url, String username, String password)
    {
        this.url = checkNotNull(url);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
    }

    public EntityManagerBuilderWithDatabaseProperties none()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.NONE);
    }

    public EntityManagerBuilderWithDatabaseProperties dbcp()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.DBCP);
    }

    public EntityManagerBuilderWithDatabaseProperties proxool()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.PROXOOL);
    }

    public EntityManagerBuilderWithDatabaseProperties dbPool()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.DBPOOL);
    }

    public EntityManagerBuilderWithDatabaseProperties c3po()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.C3PO);
    }

    public EntityManagerBuilderWithDatabaseProperties auto()
    {
        for (ConnectionPool pool : ConnectionPool.values())
        {
            if (pool.isAvailable())
            {
                return getEntityManagerBuilderWithDatabaseProperties(pool);
            }
        }
        throw new ActiveObjectsException("Could not find any connection pool! Impossible, " + ConnectionPool.NONE + " should always be an option...");
    }

    private EntityManagerBuilderWithDatabaseProperties getEntityManagerBuilderWithDatabaseProperties(ConnectionPool pool)
    {
        if (pool.isAvailable())
        {
            return new EntityManagerBuilderWithDatabaseProperties(getDatabaseProperties(pool));
        }
        else
        {
            throw new ActiveObjectsException("Connection pool " + pool + " is not available on the classpath");
        }
    }

    private BuilderDatabaseProperties getDatabaseProperties(ConnectionPool connectionPool)
    {
        return new BuilderDatabaseProperties(url, username, password, connectionPool);
    }

    private static class BuilderDatabaseProperties implements DatabaseProperties
    {
        private final String url;
        private final String username;
        private final String password;
        private final ConnectionPool pool;

        public BuilderDatabaseProperties(String url, String username, String password, ConnectionPool pool)
        {
            this.url = checkNotNull(url);
            this.username = checkNotNull(username);
            this.password = checkNotNull(password);
            this.pool = checkNotNull(pool);
        }

        public String getUrl()
        {
            return url;
        }

        public String getUsername()
        {
            return username;
        }

        public String getPassword()
        {
            return password;
        }

        public ConnectionPool getConnectionPool()
        {
            return pool;
        }
    }
}
