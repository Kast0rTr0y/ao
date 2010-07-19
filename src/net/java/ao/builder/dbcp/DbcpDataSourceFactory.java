package net.java.ao.builder.dbcp;

import net.java.ao.DisposableDataSource;
import net.java.ao.builder.ClassUtils;
import net.java.ao.builder.DataSourceFactory;
import net.java.ao.builder.DelegatingDisposableDataSource;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Driver;
import java.sql.SQLException;

public final class DbcpDataSourceFactory implements DataSourceFactory
{
    public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
    {
        final BasicDataSource dbcp = new BasicDataSource();
        dbcp.setUrl(url);
        dbcp.setUsername(username);
        dbcp.setPassword(password);
        return new DelegatingDisposableDataSource(dbcp)
        {
            public void dispose()
            {
                try
                {
                    dbcp.close();
                }
                catch (SQLException e)
                {
                    //ignored
                }
            }
        };
    }

    public static boolean isAvailable()
    {
        return ClassUtils.loadClass("org.apache.commons.dbcp.BasicDataSource") != null;
    }
}
