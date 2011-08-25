package net.java.ao.test.jdbc;

public class SqlServer extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:jtds:sqlserver://192.168.0.17:1433;DatabaseName=ao_test";
    }
}
