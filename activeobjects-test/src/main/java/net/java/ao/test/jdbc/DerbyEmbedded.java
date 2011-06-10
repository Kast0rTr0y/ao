package net.java.ao.test.jdbc;

public class DerbyEmbedded extends AbstractJdbcConfiguration
{
    @Override
    public String getUrl()
    {
        return "jdbc:derby:memory:ao_test;create=true";
    }

    @Override
    public String getUsername()
    {
        return "sa";
    }

    @Override
    public String getPassword()
    {
        return "password";
    }
}
