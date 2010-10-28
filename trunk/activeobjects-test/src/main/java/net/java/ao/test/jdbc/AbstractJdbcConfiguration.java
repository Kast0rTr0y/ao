package net.java.ao.test.jdbc;

/**
 *
 */
public abstract class AbstractJdbcConfiguration implements JdbcConfiguration
{
    public String getUsername()
    {
        return "ao_user";
    }

    public String getPassword()
    {
        return "ao_password";
    }

    @Override
    public String toString()
    {
        return new StringBuilder().append(getUrl())
                .append(" - ").append(getUsername())
                .append(" - ").append(getPassword())
                .toString();
    }
}
