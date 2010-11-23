package net.java.ao.it.config;

import net.java.ao.test.jdbc.AbstractJdbcConfiguration;
import net.java.ao.test.jdbc.Hsql;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.jdbc.MySql;
import net.java.ao.test.jdbc.Postgres;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class DynamicJdbcConfiguration extends AbstractJdbcConfiguration
{
    private static final Map<String, JdbcConfiguration> AVAILABLE = new HashMap<String, JdbcConfiguration>()
    {{
            put("hsql", new Hsql());
            put("mysql", new MySql());
            put("postgres", new Postgres());
        }};

    private final JdbcConfiguration delegate;

    public DynamicJdbcConfiguration()
    {
        final String database = ConfigurationProperties.get("ao.test.database", "hsql");
        delegate = AVAILABLE.get(database);
        if (delegate == null)
        {
            throw new IllegalStateException("Could not find appropriate database configuation for " + database);
        }
    }

    public String getUrl()
    {
        return delegate.getUrl();
    }

    public String getUsername()
    {
        return delegate.getUsername();
    }

    public String getPassword()
    {
        return delegate.getPassword();
    }
}
