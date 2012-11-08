package net.java.ao.test.jdbc;

public class SqlServer extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_SCHEMA = "ao_schema";
    public static final String DEFAULT_URL = "jdbc:jtds:sqlserver://localhost:1433;DatabaseName=ao_test";

    public SqlServer()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public SqlServer(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
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
}
