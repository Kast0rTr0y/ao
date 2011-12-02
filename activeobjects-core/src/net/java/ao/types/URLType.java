/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *          http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.types;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static net.java.ao.types.StringTypeProperties.stringType;

/**
 * @author Daniel Spiewak
 */
public class URLType extends AbstractStringType<URL>
{
    public URLType(StringTypeProperties properties)
    {
        super(properties, URL.class);
    }

    public URLType()
    {
        this(stringType("VARCHAR", "CLOB").withLength(MAX_PRECISION));
    }

    @Override
    public boolean isDefaultForSqlType()
    {
        return false;
    }

    @Override
    public boolean isAllowedAsPrimaryKey()
    {
        return true;
    }
    
    @Override
    public Object validate(Object o)
    {
        if (!(o instanceof URL))
        {
            throw new ActiveObjectsException(o + " is not of type URL");
        }
        return o;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, URL value) throws SQLException
    {
        stmt.setString(index, value.toString());
    }

    @Override
    public URL pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends URL> type, String field) throws SQLException
    {
        try
        {
            return new URL(res.getString(field));
        }
        catch (MalformedURLException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    protected URL parseValueInternal(String value)
    {
        if (StringUtils.isBlank(value))
        {
            throw new ActiveObjectsConfigurationException("Empty URLs are not allowed as default values.");
        }
        try
        {
            return new URL(value);
        }
        catch (MalformedURLException e)
        {
            throw new ActiveObjectsConfigurationException("'" + value + "' is not a valid URL");
        }
    }
}
