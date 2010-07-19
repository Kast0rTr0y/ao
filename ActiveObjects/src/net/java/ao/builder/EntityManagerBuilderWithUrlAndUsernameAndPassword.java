package net.java.ao.builder;

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

    public EntityManagerBuilderWithDatabaseProperties noPooling()
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
        return c3po();
        //return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.DBPOOL);
    }

    public EntityManagerBuilderWithDatabaseProperties c3po()
    {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.C3PO);
    }

    private EntityManagerBuilderWithDatabaseProperties getEntityManagerBuilderWithDatabaseProperties(ConnectionPool pool)
    {
        return new EntityManagerBuilderWithDatabaseProperties(getDatabaseProperties(pool));
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
