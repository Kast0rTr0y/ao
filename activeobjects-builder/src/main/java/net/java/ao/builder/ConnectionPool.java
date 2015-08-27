package net.java.ao.builder;

import net.java.ao.ActiveObjectsException;
import net.java.ao.Disposable;
import net.java.ao.DisposableDataSource;
import net.java.ao.builder.c3po.C3poDataSourceFactory;
import net.java.ao.builder.dbcp.DbcpDataSourceFactory;
import net.java.ao.builder.dbpool.DbPoolDataSourceFactory;
import net.java.ao.builder.proxool.ProxoolDataSourceFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ConnectionPool implements DataSourceFactory {
    C3PO(C3poDataSourceFactory.class),
    DBPOOL(DbPoolDataSourceFactory.class),
    PROXOOL(ProxoolDataSourceFactory.class),
    DBCP(DbcpDataSourceFactory.class),
    NONE(null) {
        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password) {
            return DelegatingDisposableDataSourceHandler.newInstance(
                    new DriverManagerDataSource(url, username, password),
                    new Disposable() {
                        public void dispose() {
                        }
                    });
        }
    };

    private final Class<? extends DataSourceFactory> dataSourceFactoryClass;

    ConnectionPool(Class<? extends DataSourceFactory> dataSourceFactoryClass) {
        this.dataSourceFactoryClass = dataSourceFactoryClass;
    }

    public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password) {
        checkNotNull(dataSourceFactoryClass);
        try {
            return dataSourceFactoryClass.newInstance().getDataSource(driverClass, url, username, password);
        } catch (InstantiationException e) {
            throw new ActiveObjectsException("Could not create an instance of <" + dataSourceFactoryClass + ">, have you called isAvailable before hand?", e);
        } catch (IllegalAccessException e) {
            throw new ActiveObjectsException("Could not create an instance of <" + dataSourceFactoryClass + ">, have you called isAvailable before hand?", e);
        }
    }

    public boolean isAvailable() {
        checkNotNull(dataSourceFactoryClass);
        try {
            return (Boolean) dataSourceFactoryClass.getMethod("isAvailable").invoke(null);
        } catch (IllegalAccessException e) {
            return false;
        } catch (InvocationTargetException e) {
            return false;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
}
