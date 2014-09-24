package net.java.ao.test.jdbc;

import com.google.common.base.Objects;

import static org.apache.commons.lang.StringUtils.defaultString;

/**
 *
 */
public abstract class AbstractJdbcConfiguration implements JdbcConfiguration
{
    public static final String DEFAULT_USER = "ao_user";
    public static final String DEFAULT_PASSWORD = "ao_password";
    private final String url;
    private final String username;
    private final String password;
    private final String schema;

    protected AbstractJdbcConfiguration(String url, String username, String password, String schema)
    {
        this.url = defaultString(url, getDefaultUrl());
        this.username = defaultString(username, getDefaultUsername());
        this.password = defaultString(password, getDefaultPassword());
        this.schema = defaultString(schema, getDefaultSchema());
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    @Override
    public String getSchema()
    {
        return schema;
    }

    public String getUrl()
    {
        return url;
    }

    @Override
    public void init()
    {
    }

    protected abstract String getDefaultSchema();

    protected abstract String getDefaultUrl();

    protected String getDefaultUsername()
    {
        return DEFAULT_USER;
    }

    protected String getDefaultPassword()
    {
        return DEFAULT_PASSWORD;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(getUsername(), getPassword(), getUrl(), getSchema());
    }

    @Override
    public final boolean equals(Object obj)
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
                && this.getUrl().equals(that.getUrl())
                && Objects.equal(this.getSchema(), that.getSchema());
    }

    @Override
    public final String toString()
    {
        return new StringBuilder().append(getUrl())
                .append(" :: ").append(getSchema() != null ? getSchema() : "<no schema>")
                .append(" - ").append(getUsername())
                .append(" - ").append(getPassword())
                .toString();
    }
}
