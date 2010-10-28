package net.java.ao.builder;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class DriverManagerDataSource implements DataSource
{
    private final String url;
    private final String username;
    private final String password;

    private PrintWriter out;
    private int loginTimeOut;

    public DriverManagerDataSource(String url, String username, String password)
    {
        this.url = checkNotNull(url);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
    }

    public Connection getConnection() throws SQLException
    {
        return getConnection(this.username, this.password);
    }

    public Connection getConnection(String username, String password) throws SQLException
    {
        return DriverManager.getConnection(url, username, password);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        return iface.cast(this);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return this.getClass().equals(iface);
    }

    public PrintWriter getLogWriter() throws SQLException
    {
        return out;
    }

    public void setLogWriter(PrintWriter out) throws SQLException
    {
        this.out = out;
    }

    public void setLoginTimeout(int seconds) throws SQLException
    {
        this.loginTimeOut = seconds;
    }

    public int getLoginTimeout() throws SQLException
    {
        return loginTimeOut;
    }
}
