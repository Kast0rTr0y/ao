package net.java.ao.test.jdbc;

/**
 *
 */
public class Postgres extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:postgresql://localhost:5432/ao_test";
    }

    @Override
    public String getSchema()
    {
        return "ao_schema";
    }
}
