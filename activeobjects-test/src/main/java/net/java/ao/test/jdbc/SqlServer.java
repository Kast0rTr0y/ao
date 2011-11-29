package net.java.ao.test.jdbc;

public class SqlServer extends AbstractJdbcConfiguration
{
    @Override
    public String getSchema()
    {
        return "ao_schema";
    }

    public String getUrl()
    {
        return "jdbc:jtds:sqlserver://192.168.0.160:1433;DatabaseName=ao_test";
    }
}
