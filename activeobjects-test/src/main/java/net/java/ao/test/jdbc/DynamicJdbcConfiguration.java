package net.java.ao.test.jdbc;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import net.java.ao.test.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A JDBC configuration that can be configured through either system properties or a configuration file.</p>
 * <p>The default database used if no configuration was found is {@link Hsql}</p>
 *
 * @see ConfigurationProperties
 */
public final class DynamicJdbcConfiguration extends AbstractJdbcConfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicJdbcConfiguration.class);

    private static final ImmutableMap<String, JdbcConfiguration> CONFIGS = ImmutableMap.<String, JdbcConfiguration>builder()
            .put("hsql", new Hsql())
            .put("mysql", new MySql())
            .put("postgres", new Postgres())
            .put("oracle", new Oracle())
            .put("sqlserver", new SqlServer())
            .put("derby-embedded", new DerbyEmbedded())
            .build();

    private static final String DEFAULT = "hsql";

    private final Supplier<JdbcConfiguration> jdbcSupplier;

    public DynamicJdbcConfiguration()
    {
        this.jdbcSupplier = Suppliers.memoize(new SystemPropertyJdbcConfigurationSupplier());
    }

    @Override
    public String getUrl()
    {
        return jdbcSupplier.get().getUrl();
    }

    @Override
    public String getUsername()
    {
        return jdbcSupplier.get().getUsername();
    }

    @Override
    public String getPassword()
    {
        return jdbcSupplier.get().getPassword();
    }

    @Override
    public String getSchema()
    {
        return jdbcSupplier.get().getSchema();
    }

    private static final class SystemPropertyJdbcConfigurationSupplier implements Supplier<JdbcConfiguration>
    {
        @Override
        public JdbcConfiguration get()
        {
            final String db = ConfigurationProperties.get("ao.test.database", DEFAULT);
            final JdbcConfiguration jdbcConfiguration = CONFIGS.get(db);

            logger.debug("JDBC configuration key is {} and resolved to {}", db, jdbcConfiguration);
            return jdbcConfiguration;
        }
    }
}
