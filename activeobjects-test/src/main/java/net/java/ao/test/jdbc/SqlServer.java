package net.java.ao.test.jdbc;

public class SqlServer extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:jtds:sqlserver://localhost:1433;DatabaseName=ao_test";
    }
}
