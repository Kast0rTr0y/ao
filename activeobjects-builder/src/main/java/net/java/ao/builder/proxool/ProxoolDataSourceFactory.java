package net.java.ao.builder.proxool;

import net.java.ao.Disposable;
import net.java.ao.DisposableDataSource;
import net.java.ao.builder.ClassUtils;
import net.java.ao.builder.DataSourceFactory;
import net.java.ao.builder.DelegatingDisposableDataSourceHandler;
import org.logicalcobwebs.proxool.ProxoolDataSource;
import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.ProxoolFacade;

import java.sql.Driver;

public class ProxoolDataSourceFactory implements DataSourceFactory
{
    private static final String ALIAS = "active-objects";

    public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
    {
        final ProxoolDataSource source = new ProxoolDataSource(ALIAS);
        source.setUser(username);
        source.setPassword(password);
        source.setDriver(driverClass.getName());
        source.setDriverUrl(url);
        source.setMaximumConnectionCount(30);

        return DelegatingDisposableDataSourceHandler.newInstance(source, new Disposable()
        {
            @Override
            public void dispose()
            {
                try
                {
                    ProxoolFacade.removeConnectionPool(ALIAS);
                }
                catch (ProxoolException e)
                {
                    // ignored
                }
            }
        });
    }

    public static boolean isAvailable()
    {
        return ClassUtils.loadClass("org.logicalcobwebs.proxool.ProxoolDriver") != null;
    }
}
