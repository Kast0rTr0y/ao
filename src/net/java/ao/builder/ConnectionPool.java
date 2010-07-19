package net.java.ao.builder;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import net.java.ao.DisposableDataSource;
import net.java.ao.ActiveObjectsException;
import org.apache.commons.dbcp.BasicDataSource;
import snaq.db.DBPoolDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public enum ConnectionPool implements DataSourceFactory
{
    DBPOOL
            {
                public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    DBPoolDataSource ds = new DBPoolDataSource();
                    ds.setName("active-objects");
                    ds.setDriverClassName(driverClass.getName());
                    ds.setUrl(url);
                    ds.setUsername(username);
                    ds.setPassword(password);
                    ds.setPoolSize(5);
                    ds.setMaxSize(30);
                    ds.setExpiryTime(3600);  // Specified in seconds.
                    return new DelegatingDisposableDataSource(ds);
                }
            },
    C3PO
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
                        @Override
                        public void dispose()
                        {
                            try
                            {
                                DataSources.destroy(getDelegate());
                            }
                            catch (SQLException ignored)
                            {
                                // ignored
                            }
                        }
                    };
                }
            },
    PROXOOL
            {
                public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            },
    DBCP
            {
                public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    final BasicDataSource dbcp = new BasicDataSource();
                    dbcp.setUrl(url);
                    dbcp.setUsername(username);
                    dbcp.setPassword(password);
                    return new DelegatingDisposableDataSource(dbcp);
                }
            },
    NONE
            {
                public DisposableDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    return new DelegatingDisposableDataSource(new DriverManagerDataSource(url, username, password));
                }
            };

    private static class DelegatingDisposableDataSource implements DisposableDataSource
    {
        private final DataSource delegate;

        DelegatingDisposableDataSource(DataSource delegate)
        {
            this.delegate = checkNotNull(delegate);
        }

        DataSource getDelegate()
        {
            return delegate;
        }

        public void dispose()
        {
        }

        public Connection getConnection() throws SQLException
        {
            return delegate.getConnection();
        }

        public Connection getConnection(String username, String password) throws SQLException
        {
            return delegate.getConnection(username, password);
        }

        public <T> T unwrap(Class<T> iface) throws SQLException
        {
            return delegate.unwrap(iface);
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException
        {
            return delegate.isWrapperFor(iface);
        }

        public PrintWriter getLogWriter() throws SQLException
        {
            return delegate.getLogWriter();
        }

        public void setLogWriter(PrintWriter out) throws SQLException
        {
            delegate.setLogWriter(out);
        }

        public void setLoginTimeout(int seconds) throws SQLException
        {
            delegate.setLoginTimeout(seconds);
        }

        public int getLoginTimeout() throws SQLException
        {
            return delegate.getLoginTimeout();
        }
    }
}
