package net.java.ao.test.jdbc;

/**
 * Used with the {@link Jdbc @Jdbc} annotation to specify JDBC parameters for unit testing.
 */
public interface JdbcConfiguration {
    String getUrl();

    String getSchema();

    String getUsername();

    String getPassword();
}
