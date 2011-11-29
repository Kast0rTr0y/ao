package net.java.ao.test.jdbc;

/**
 *
 */
public class Oracle extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:oracle:thin:@localhost:1521:orcl";
    }

    @Override
    public String getSchema()
    {
        return "ao_schema";
    }
}
