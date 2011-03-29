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

    public DataSource getDelegate()
    {
        return delegate;
    }

    public Connection getConnection() throws SQLException
    {
        return delegate.getConnection();
    }

    public Connection getConnection(String username, String password) throws SQLException
    {
        return delegate.getConnection(username, password);
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
