/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.types;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.EntityManager;
import net.java.ao.util.StringUtils;

import static net.java.ao.types.StringTypeProperties.stringType;

/**
 * @author Daniel Spiewak
 */
public class VarcharType extends AbstractStringType<String>
{
    public VarcharType(StringTypeProperties properties)
    {
        super(properties, String.class);
    }

    public VarcharType()
    {
        this(stringType("VARCHAR", "CLOB"));
    }

    @Override
    public String pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends String> type, String field) throws SQLException
    {
        return res.getString(field);
    }

    @Override
    protected String parseValueInternal(String value)
    {
        if (StringUtils.isBlank(value))
        {
            throw new ActiveObjectsConfigurationException("Empty strings are not supported on all databases. Therefore is not supported by Active Objects.");
        }

        return value;
    }
}
