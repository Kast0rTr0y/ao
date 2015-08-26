package net.java.ao.builder;

public interface DatabaseProperties {
    String getUrl();

    String getUsername();

    String getPassword();

    String getSchema();

    ConnectionPool getConnectionPool();
}
