package net.java.ao.test.jdbc;

public class Hsql extends AbstractJdbcConfiguration
{
    public String getUrl()
    {
        return "jdbc:hsqldb:mem:ao_test";
    }

    public String getUsername()
    {
        return "sa";
    }

    public String getPassword()
    {
        return "";
    }
}
