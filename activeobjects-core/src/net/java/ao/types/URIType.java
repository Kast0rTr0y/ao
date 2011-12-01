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

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author Nathan Hamblen
 */
public class URIType extends DatabaseType<URI>
{
    public URIType(String sqlTypeIdentifier)
    {
        super(Types.VARCHAR, VarcharType.MAX_PRECISION, sqlTypeIdentifier, URI.class);
    }

    public URIType()
    {
        this("VARCHAR");
    }

    @Override
    public boolean isDefaultForSqlType()
    {
        return false;
    }
    
    @Override
    public Object validate(Object o)
    {
        if (!(o instanceof URI))
        {
            throw new ActiveObjectsException(o + " is not of type URI");
        }
        return o;
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, URI value) throws SQLException
    {
        stmt.setString(index, value.toString());
    }

    @Override
    public URI pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends URI> type, String field) throws SQLException
    {
        try
        {
            return new URI(res.getString(field));
        }
        catch (URISyntaxException e)
        {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public URI defaultParseValue(String value)
    {
        if (StringUtils.isBlank(value))
        {
            throw new ActiveObjectsConfigurationException("Empty URIs are not allowed as default values.");
        }
        try
        {
            return new URI(value);
        }
        catch (URISyntaxException e)
        {
            throw new ActiveObjectsConfigurationException("'" + value + "' is not a valid URI");
        }
    }
}