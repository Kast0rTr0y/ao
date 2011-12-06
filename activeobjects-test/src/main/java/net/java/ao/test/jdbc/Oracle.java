package net.java.ao.test.jdbc;

/**
 *
 */
public class Oracle extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:oracle:thin:@192.168.0.113:1521:xe";
    }

    @Override
    public String getSchema()
    {
        return "ao_schema";
    }
}
