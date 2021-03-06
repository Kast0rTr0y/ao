package net.java.ao.test.jdbc;

public class H2Memory extends AbstractJdbcConfiguration {
    private static final String DEFAULT_URL = "jdbc:h2:mem:ao-test;MVCC=TRUE";
    private static final String DEFAULT_USER = "";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    public H2Memory() {
        this(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public H2Memory(String url, String username, String password, String schema) {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultUsername() {
        return DEFAULT_USER;
    }

    @Override
    protected String getDefaultPassword() {
        return DEFAULT_PASSWORD;
    }

    @Override
    protected String getDefaultSchema() {
        return DEFAULT_SCHEMA;
    }

    @Override
    protected String getDefaultUrl() {
        return DEFAULT_URL;
    }
}
