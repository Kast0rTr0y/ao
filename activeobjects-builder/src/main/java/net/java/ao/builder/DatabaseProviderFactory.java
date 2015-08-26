package net.java.ao.builder;

import net.java.ao.DatabaseProvider;

class DatabaseProviderFactory {
    static DatabaseProvider getDatabaseProvider(DatabaseProperties databaseProperties) {
        final SupportedDatabase supportedDb = SupportedDatabase.fromUri(databaseProperties.getUrl());
        return supportedDb.getDatabaseProvider(
                databaseProperties.getConnectionPool(),
                databaseProperties.getUrl(),
                databaseProperties.getUsername(),
                databaseProperties.getPassword(),
                databaseProperties.getSchema());
    }
}
