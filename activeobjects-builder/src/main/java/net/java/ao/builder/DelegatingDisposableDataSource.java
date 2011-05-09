package net.java.ao.builder;

import net.java.ao.DisposableDataSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class DelegatingDisposableDataSource implements DisposableDataSource
{
    private final DataSource delegate;

    public DelegatingDisposableDataSource(DataSource delegate)
    {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public Connection getConnection() throws SQLException
    {
        return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException
    {
        return delegate.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException
    {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException
    {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException
    {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException
    {
        return delegate.getLoginTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return delegate.isWrapperFor(iface);
    }
}
