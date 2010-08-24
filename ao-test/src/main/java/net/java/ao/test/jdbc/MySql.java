package net.java.ao.test.jdbc;

/**
 *
 */
public class MySql extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:mysql://localhost:3306/ao_test?autoReconnect=true";
    }
}
