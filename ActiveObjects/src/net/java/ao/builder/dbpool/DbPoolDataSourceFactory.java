package net.java.ao.builder.dbpool;

import net.java.ao.DisposableDataSource;
import net.java.ao.builder.ClassUtils;
import net.java.ao.builder.DataSourceFactory;
import net.java.ao.builder.DelegatingDisposableDataSource;
import snaq.db.DBPoolDataSource;

import java.sql.Driver;

public final class DbPoolDataSourceFactory implements DataSourceFactory
{
    public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
    {
        final DBPoolDataSource ds = new DBPoolDataSource();
        ds.setName("active-objects");
        ds.setDriverClassName(driverClass.getName());
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setPoolSize(5);
        ds.setMaxSize(30);
        ds.setExpiryTime(3600);  // Specified in seconds.
        return new DelegatingDisposableDataSource(ds)
        {
            public void dispose()
            {
                ds.releaseConnectionPool();
            }
        };
    }

    public static boolean isAvailable()
    {
        return ClassUtils.loadClass("snaq.db.ConnectionPool") != null;
    }
}
