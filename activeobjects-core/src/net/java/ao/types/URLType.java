package net.java.ao.types;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.VARCHAR;

import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

final class URLType extends AbstractLogicalType<URL>
{
    public URLType()
    {
        super("URL",
              new Class<?>[] { URL.class },
              VARCHAR, new Integer[] { });
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, URL value, int jdbcType) throws SQLException
    {
        stmt.setString(index, value.toString());
    }

    @Override
    public URL pullFromDatabase(EntityManager manager, ResultSet res, Class<URL> type, String columnName) throws SQLException
    {
        try
        {
            return new URL(res.getString(columnName));
        }
        catch (MalformedURLException e)
        {
            throw new SQLException(e.getMessage());
        }
    }        
    
    @Override
    public URL parse(String input)
    {
        try
        {
            return StringUtils.isBlank(input) ? null : new URL(input);
        }
        catch (MalformedURLException e)
        {
            throw new IllegalArgumentException("'" + input + "' is not a valid URI");
        }
    }
}