package net.java.ao.builder;

import net.java.ao.ActiveObjectsException;

import static com.google.common.base.Preconditions.checkNotNull;

public class UnloadableJdbcDriverException extends ActiveObjectsException {
    private final String driverClassName;

    public UnloadableJdbcDriverException(String driverClassName) {
        this(driverClassName, null);
    }

    public UnloadableJdbcDriverException(String driverClassName, Throwable t) {
        super(t);
        this.driverClassName = checkNotNull(driverClassName);
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    @Override
    public String getMessage() {
        return "Could not load JDBC driver <" + driverClassName + ">";
    }
}
