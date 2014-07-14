package net.java.ao.test.jdbc;

/**
 *
 */
public class NuoDB extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_URL = "jdbc:com.nuodb://localhost/ao_test";

    public NuoDB()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, null);
    }

    public NuoDB(String url, String username, String password, String schema)
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
