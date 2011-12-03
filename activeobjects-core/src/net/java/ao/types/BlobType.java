package net.java.ao.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.LONGVARBINARY;

import static java.sql.Types.VARBINARY;

import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;

import static java.sql.Types.BINARY;
import static java.sql.Types.BLOB;

final class BlobType extends AbstractLogicalType<Object>
{
    public BlobType()
    {
        super("Blob",
              new Class<?>[] { byte[].class, InputStream.class },
              BLOB, new Integer[] { BLOB, BINARY, VARBINARY, LONGVARBINARY });
    }
    
    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Object value, int jdbcType) throws SQLException
    {        
        final InputStream is;
        if (value instanceof byte[])
        {
            is = new ByteArrayInputStream((byte[]) value);
        }
        else if (value instanceof InputStream)
        {
            is = (InputStream) value;
        }
        else
        {
            throw new IllegalArgumentException("BLOB value must be of type byte[] or InputStream");
        }

        try
        {
            stmt.setBinaryStream(index, is, is.available());
        }
        catch (IOException e)
        {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public Object pullFromDatabase(EntityManager manager, ResultSet res, Class<Object> type, String columnName)
        throws SQLException
    {
        return manager.getProvider().handleBlob(res, type, columnName);
    }
    
    @Override
    public boolean shouldCache(Class<?> type)
    {
        return !InputStream.class.isAssignableFrom(type) && super.shouldCache(type);
    }

    @Override
    public String valueToString(Object value)
    {
        throw new ActiveObjectsException("Cannot convert BLOB value to string");
    }
}
