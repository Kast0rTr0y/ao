package net.java.ao.test.jdbc;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import net.java.ao.test.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.java.ao.util.StringUtils.isBlank;

/**
 * <p>A JDBC configuration that can be configured through either system properties or a configuration file.</p>
 * <p>The default database used if no configuration was found is {@link Hsql}</p>
 *
 * @see ConfigurationProperties
 */
public final class DynamicJdbcConfiguration extends AbstractJdbcConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(DynamicJdbcConfiguration.class);

    private static final ImmutableMap<String, JdbcConfiguration> CONFIGS = ImmutableMap.<String, JdbcConfiguration>builder()
            .put("hsql", new Hsql())
            .put("h2-memory", new H2Memory())
            .put("h2-file", new H2File())
            .put("h2-server", new H2Server())
            .put("hsql-file", new HsqlFileStorage())
            .put("mysql", new MySql())
            .put("postgres", new Postgres())
            .put("oracle", new Oracle())
            .put("sqlserver", new SqlServer())
            .put("derby-embedded", new DerbyEmbedded())
            .build();

    private static final String DEFAULT = "hsql";

    private static final Supplier<JdbcConfiguration> jdbcSupplier = Suppliers.memoize(new SystemPropertyJdbcConfigurationSupplier());


    public DynamicJdbcConfiguration() {
        super(jdbcSupplier.get().getUrl(), jdbcSupplier.get().getUsername(), jdbcSupplier.get().getPassword(), jdbcSupplier.get().getSchema());
    }

    protected DynamicJdbcConfiguration(String url, String username, String password, String schema) {
        super(url, username, password, schema);
    }

    @Override
    public String getUrl() {
        return jdbcSupplier.get().getUrl();
    }

    @Override
    protected String getDefaultSchema() {
        return jdbcSupplier.get().getSchema();
    }

    @Override
    protected String getDefaultUrl() {
        return jdbcSupplier.get().getUrl();
    }

    @Override
    public String getUsername() {
        return jdbcSupplier.get().getUsername();
    }

    @Override
    public String getPassword() {
        return jdbcSupplier.get().getPassword();
    }

    @Override
    public String getSchema() {
        return jdbcSupplier.get().getSchema();
    }

    private static final class SystemPropertyJdbcConfigurationSupplier implements Supplier<JdbcConfiguration> {
        @Override
        public JdbcConfiguration get() {
            final String db = ConfigurationProperties.get("ao.test.database", DEFAULT);
            final JdbcConfiguration jdbcConfiguration = buildJdbcConfiguration(db);

            logger.debug("JDBC configuration key is {} and resolved to {}", db, jdbcConfiguration);

            jdbcConfiguration.init();

            return jdbcConfiguration;
        }

        private JdbcConfiguration buildJdbcConfiguration(String db) {
            JdbcConfiguration jdbcConfiguration = null;

            String username = ConfigurationProperties.get("db.username", null);
            String password = ConfigurationProperties.get("db.password", null);
            String dbUrl = ConfigurationProperties.get("db.url", null);
            String dbSchema = ConfigurationProperties.get("db.schema", null);


            if (!isBlank(username) || !isBlank(password) || !isBlank(dbUrl) || !isBlank(dbSchema)) {
                if ("postgres".equals(db)) {
                    jdbcConfiguration = new Postgres(dbUrl, username, password, dbSchema);
                } else if ("hsql".equals(db)) {
                    jdbcConfiguration = new Hsql(dbUrl, username, password, dbSchema);
                } else if ("h2-memory".equals(db)) {
                    jdbcConfiguration = new H2Memory(dbUrl, username, password, dbSchema);
                } else if ("h2-file".equals(db)) {
                    jdbcConfiguration = new H2File(dbUrl, username, password, dbSchema);
                } else if ("h2-server".equals(db)) {
                    jdbcConfiguration = new H2Server(dbUrl, username, password, dbSchema);
                } else if ("hsql-file".equals(db)) {
                    jdbcConfiguration = new HsqlFileStorage(dbUrl, username, password, dbSchema);
                } else if ("mysql".equals(db)) {
                    jdbcConfiguration = new MySql(dbUrl, username, password, dbSchema);
                } else if ("oracle".equals(db)) {
                    jdbcConfiguration = new Oracle(dbUrl, username, password, dbSchema);
                } else if ("sqlserver".equals(db)) {
                    jdbcConfiguration = new SqlServer(dbUrl, username, password, dbSchema);
                } else if ("derby-embedded".equals(db)) {
                    jdbcConfiguration = new DerbyEmbedded(dbUrl, username, password, dbSchema);
                }
            } else {
                jdbcConfiguration = CONFIGS.get(db);
            }
            return jdbcConfiguration;
        }
    }
}
