package net.java.ao.test.jdbc;

public class Hsql extends AbstractJdbcConfiguration
{
    private static final String IN_MEMORY_URL = "jdbc:hsqldb:mem:ao_test";
    private static final String DEFAULT_USERNAME = "sa";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    public Hsql()
    {
        this(IN_MEMORY_URL);
    }

    protected Hsql(String url)
    {
        this(url, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public Hsql(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultUsername()
    {
        return DEFAULT_USERNAME;
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
        return IN_MEMORY_URL;
    }
}
