package net.java.ao.test.jdbc;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>A JDBC configuration that can be configured through either system properties or a configuration file.</p>
 * <p>The default database used if no configuration was found is {@link Hsql}</p>
 *
 * @see ConfigurationProperties
 */
public final class DynamicJdbcConfiguration extends AbstractJdbcConfiguration
{
    private static final Map<String, JdbcConfiguration> AVAILABLE = new HashMap<String, JdbcConfiguration>()
    {{
            put("hsql", new Hsql());
            put("mysql", new MySql());
            put("postgres", new Postgres());
            put("oracle", new Oracle());
        }};

    private final JdbcConfiguration delegate;

    public DynamicJdbcConfiguration()
    {
        final String database = ConfigurationProperties.get("ao.test.database", "hsql");
        delegate = get(database);
        if (delegate == null)
        {
            throw new IllegalStateException("Could not find appropriate database configuration for " + database);
        }
    }

    private JdbcConfiguration get(String database)
    {
        return AVAILABLE.get(database);
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
