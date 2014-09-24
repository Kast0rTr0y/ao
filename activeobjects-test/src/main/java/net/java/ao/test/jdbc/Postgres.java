package net.java.ao.test.jdbc;

/**
 *
 */
public class Postgres extends AbstractJdbcConfiguration
{

    public static final String DEFAULT_SCHEMA = "ao_schema";
    public static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/ao_test";

    public Postgres()
    {
        super(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public Postgres(String url, String username, String password, String schema)
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
