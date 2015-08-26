package net.java.ao.builder;

import net.java.ao.DisposableDataSource;

import java.sql.Driver;

/**
 * Allow creation of a {@link net.java.ao.DisposableDataSource disposable data source} from given
 * connection properties.
 */
public interface DataSourceFactory {
    DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password);
}
