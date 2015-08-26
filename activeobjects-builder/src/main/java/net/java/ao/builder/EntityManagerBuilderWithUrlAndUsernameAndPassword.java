package net.java.ao.builder;

import net.java.ao.ActiveObjectsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class EntityManagerBuilderWithUrlAndUsernameAndPassword {
    private final Logger logger = LoggerFactory.getLogger(EntityManagerBuilder.class);

    private final String url, username, password;
    private String schema;

    public EntityManagerBuilderWithUrlAndUsernameAndPassword(String url, String username, String password) {
        this.url = checkNotNull(url);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
    }

    public EntityManagerBuilderWithUrlAndUsernameAndPassword schema(String schema) {
        this.schema = schema;
        return this;
    }

    public EntityManagerBuilderWithDatabaseProperties none() {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.NONE);
    }

    public EntityManagerBuilderWithDatabaseProperties dbcp() {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.DBCP);
    }

    public EntityManagerBuilderWithDatabaseProperties proxool() {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.PROXOOL);
    }

    public EntityManagerBuilderWithDatabaseProperties dbPool() {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.DBPOOL);
    }

    public EntityManagerBuilderWithDatabaseProperties c3po() {
        return getEntityManagerBuilderWithDatabaseProperties(ConnectionPool.C3PO);
    }

    public EntityManagerBuilderWithDatabaseProperties auto() {
        for (ConnectionPool pool : ConnectionPool.values()) {
            if (pool.isAvailable()) {
                return getEntityManagerBuilderWithDatabaseProperties(pool);
            }
        }
        throw new ActiveObjectsException("Could not find any connection pool! Impossible, " + ConnectionPool.NONE + " should always be an option...");
    }

    private EntityManagerBuilderWithDatabaseProperties getEntityManagerBuilderWithDatabaseProperties(ConnectionPool pool) {
        if (pool.isAvailable()) {
            logger.debug("Entity manager will be using connection pool '{}'.", pool);
            return new EntityManagerBuilderWithDatabaseProperties(getDatabaseProperties(pool));
        } else {
            throw new ActiveObjectsException("Connection pool " + pool + " is not available on the classpath");
        }
    }

    private BuilderDatabaseProperties getDatabaseProperties(ConnectionPool connectionPool) {
        final BuilderDatabaseProperties properties = new BuilderDatabaseProperties(url, username, password, connectionPool);
        properties.setSchema(schema);
        return properties;
    }
}
