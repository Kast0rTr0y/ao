package net.java.ao.sql;

import net.java.ao.sql.CloseableResultSetMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public abstract class AbstractCloseableResultSetMetaData implements CloseableResultSetMetaData
{
    private final ResultSetMetaData delegate;

    public AbstractCloseableResultSetMetaData(ResultSetMetaData delegate)
    {
        this.delegate = delegate;
    }

    public int getColumnCount() throws SQLException
    {
        return delegate.getColumnCount();
    }

    public boolean isAutoIncrement(int column) throws SQLException
    {
        return delegate.isAutoIncrement(column);
    }

    public boolean isCaseSensitive(int column) throws SQLException
    {
        return delegate.isCaseSensitive(column);
    }

    public boolean isSearchable(int column) throws SQLException
    {
        return delegate.isSearchable(column);
    }

    public boolean isCurrency(int column) throws SQLException
    {
        return delegate.isCurrency(column);
    }

    public int isNullable(int column) throws SQLException
    {
        return delegate.isNullable(column);
    }

    public boolean isSigned(int column) throws SQLException
    {
        return delegate.isSigned(column);
    }

    public int getColumnDisplaySize(int column) throws SQLException
    {
        return delegate.getColumnDisplaySize(column);
    }

    public String getColumnLabel(int column) throws SQLException
    {
        return delegate.getColumnLabel(column);
    }

    public String getColumnName(int column) throws SQLException
    {
        return delegate.getColumnName(column);
    }

    public String getSchemaName(int column) throws SQLException
    {
        return delegate.getSchemaName(column);
    }

    public int getPrecision(int column) throws SQLException
    {
        return delegate.getPrecision(column);
    }

    public int getScale(int column) throws SQLException
    {
        return delegate.getScale(column);
    }

    public String getTableName(int column) throws SQLException
    {
        return delegate.getTableName(column);
    }

    public String getCatalogName(int column) throws SQLException
    {
        return delegate.getCatalogName(column);
    }

    public int getColumnType(int column) throws SQLException
    {
        return delegate.getColumnType(column);
    }

    public String getColumnTypeName(int column) throws SQLException
    {
        return delegate.getColumnTypeName(column);
    }

    public boolean isReadOnly(int column) throws SQLException
    {
        return delegate.isReadOnly(column);
    }

    public boolean isWritable(int column) throws SQLException
    {
        return delegate.isWritable(column);
    }

    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        return delegate.isDefinitelyWritable(column);
    }

    public String getColumnClassName(int column) throws SQLException
    {
        return delegate.getColumnClassName(column);
    }
}
