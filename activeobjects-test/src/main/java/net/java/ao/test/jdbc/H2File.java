package net.java.ao.test.jdbc;

import com.google.common.io.Files;

import java.io.File;

public class H2File extends AbstractJdbcConfiguration {
    private static final File TEMP_DIR = Files.createTempDir();

    private static final String DEFAULT_URL = "jdbc:h2:file:" + TEMP_DIR + "/ao-test;MVCC=TRUE";
    private static final String DEFAULT_USER = "";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    public H2File() {
        this(DEFAULT_URL, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public H2File(String url, String username, String password, String schema) {
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
