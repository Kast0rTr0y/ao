package net.java.ao.builder;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import net.java.ao.ActiveObjectsDataSource;
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
                public ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
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
                    return new DelegatingActiveObjectsDataSource(ds, driverClass, url, username, password);
                }
            },
    C3PO
            {
                public ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
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
                    
                    return new DelegatingActiveObjectsDataSource(cpds, driverClass, url, username, password)
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
                public ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    return null;  //To change body of implemented methods use File | Settings | File Templates.
                }
            },
    DBCP
            {
                public ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    final BasicDataSource dbcp = new BasicDataSource();
                    dbcp.setUrl(url);
                    dbcp.setUsername(username);
                    dbcp.setPassword(password);
                    return new DelegatingActiveObjectsDataSource(dbcp, driverClass, url, username, password);
                }
            },
    NONE
            {
                public ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password)
                {
                    return new DelegatingActiveObjectsDataSource(new DriverManagerDataSource(url, username, password), driverClass, url, username, password);
                }
            };

    private static class DelegatingActiveObjectsDataSource implements ActiveObjectsDataSource
    {
        private final DataSource delegate;
        private final String url;
        private final String username;
        private final String password;
        private final Class<? extends Driver> driverClass;

        DelegatingActiveObjectsDataSource(DataSource delegate, Class<? extends Driver> driverClass, String url, String username, String password)
        {
            this.delegate = checkNotNull(delegate);
            this.driverClass = checkNotNull(driverClass);
            this.url = checkNotNull(url);
            this.username = checkNotNull(username);
            this.password = checkNotNull(password);
        }

        DataSource getDelegate()
        {
            return delegate;
        }

        public String getUrl()
        {
            return url;
        }

        public String getUsername()
        {
            return username;
        }

        public String getPassword()
        {
            return password;
        }

        public Class<? extends Driver> getDriverClass()
        {
            return driverClass;
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
