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
    public int hashCode()
    {
        return 11 * getUsername().hashCode()
                + 7 * getPassword().hashCode()
                + getUrl().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof JdbcConfiguration))
        {
            return false;
        }

        final JdbcConfiguration that = (JdbcConfiguration) obj;
        return this.getUsername().equals(that.getUsername())
                && this.getPassword().equals(that.getPassword())
                && this.getUrl().equals(that.getUrl());
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
