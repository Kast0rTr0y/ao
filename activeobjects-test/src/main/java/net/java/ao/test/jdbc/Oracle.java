package net.java.ao.test.jdbc;

/**
 *
 */
public class Oracle extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_SCHEMA = "ao_schema";
    public static final String DEFAULT_URL = "jdbc:oracle:thin:@localhost:1521:orcl";

    public Oracle(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    public Oracle()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
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
    public void init()
    {
    }
}
