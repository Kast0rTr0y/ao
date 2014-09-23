package net.java.ao.test.jdbc;

import org.h2.tools.Server;

import java.sql.SQLException;

public class H2Server extends AbstractJdbcConfiguration
{
    private static final String DEFAULT_URL = "jdbc:h2:tcp://localhost/./target/ao-test;MVCC=TRUE";
    private static final String DEFAULT_USER = "";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    private static Server h2Server;

    public H2Server()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public H2Server(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultUsername()
    {
        return DEFAULT_USER;
    }

    @Override
    protected String getDefaultPassword()
    {
        return DEFAULT_PASSWORD;
    }

    @Override
    protected String getDefaultSchema()
    {
        return DEFAULT_SCHEMA;
    }

    @Override
    protected String getDefaultUrl()
    {
        return DEFAULT_URL;
    }

    @Override
    public synchronized void init()
    {
        if (h2Server == null)
        {
            // launch an H2 server if there isn't one
            try
            {
                h2Server = Server.createTcpServer().start();
            }
            catch (SQLException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
