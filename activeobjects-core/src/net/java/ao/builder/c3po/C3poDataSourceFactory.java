package net.java.ao.builder.c3po;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import net.java.ao.ActiveObjectsException;
import net.java.ao.DisposableDataSource;
import net.java.ao.builder.ClassUtils;
import net.java.ao.builder.DataSourceFactory;
import net.java.ao.builder.DelegatingDisposableDataSource;

import java.beans.PropertyVetoException;
import java.sql.Driver;
import java.sql.SQLException;

public class C3poDataSourceFactory implements DataSourceFactory
{
    public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
    {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();
        try
        {
            cpds.setDriverClass(driverClass.getName());
        }
        catch (PropertyVetoException e)
        {
            throw new ActiveObjectsException(e);
        }
        cpds.setJdbcUrl(url);
        cpds.setUser(username);
        cpds.setPassword(password);
        cpds.setMaxPoolSize(30);
        cpds.setMaxStatements(180);

        return new DelegatingDisposableDataSource(cpds)
        {
            public void dispose()
            {
                try
                {
                    DataSources.destroy(cpds);
                }
                catch (SQLException ignored)
                {
                    // ignored
                }
            }
        };
    }

    public static boolean isAvailable()
    {
        return ClassUtils.loadClass("com.mchange.v2.c3p0.ComboPooledDataSource") != null;
    }
}