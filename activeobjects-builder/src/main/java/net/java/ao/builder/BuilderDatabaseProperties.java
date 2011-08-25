package net.java.ao.builder;

import static com.google.common.base.Preconditions.checkNotNull;

final class BuilderDatabaseProperties implements DatabaseProperties
{
    private final String url;
    private final String username;
    private final String password;
    private final ConnectionPool pool;
    private String schema = null;

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

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }
}
