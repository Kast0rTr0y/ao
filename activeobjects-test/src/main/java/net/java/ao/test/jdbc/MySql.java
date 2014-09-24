package net.java.ao.test.jdbc;

/**
 *
 */
public class MySql extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_URL = "jdbc:mysql://localhost:3306/ao_test?autoReconnect=true";

    public MySql()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, null);
    }

    public MySql(String url, String username, String password, String schema)
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
}
