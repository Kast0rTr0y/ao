package net.java.ao.test.jdbc;

public class DerbyEmbedded extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_URL = "jdbc:derby:memory:ao_test;create=true";
    public static final String DEFAULT_USER = "sa";
    public static final String DEFAULT_PASSWORD = "password";

    public DerbyEmbedded()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, null);
    }

    public DerbyEmbedded(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultSchema()
    {
        return null;
    }

    @Override
    protected String getDefaultUrl()
    {
        return DEFAULT_URL;
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
    public void init()
    {
    }
}
