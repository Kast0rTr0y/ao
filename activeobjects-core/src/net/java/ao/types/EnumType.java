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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.NoSuchElementException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;

import static net.java.ao.types.NumericTypeProperties.numericType;
import static net.java.ao.util.EnumUtils.values;

/**
 * @author Daniel Spiewak
 */
public class EnumType extends AbstractNumericType<Enum<?>>
{
    public EnumType(NumericTypeProperties properties)
    {
        super(Types.INTEGER, properties, Enum.class);
    }
    
    public EnumType()
    {
        this(numericType("INTEGER"));
    }
    
    @Override
    public boolean isDefaultForSqlType()
    {
        return false;
    }

    @Override
    public Enum<?> pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Enum<?>> type, String field) throws SQLException
    {
        final int dbValue = res.getInt(field);
        try
        {
            return findEnum(type, dbValue);
        }
        catch (NoSuchElementException e)
        {
            throw new ActiveObjectsException("Could not find enum value for '" + type + "' corresponding to database value '" + dbValue + "'");
        }
    }

    private Enum findEnum(Class<? extends Enum<?>> type, final int dbValue)
    {
        return Iterables.find(values(type), new Predicate<Enum>()
        {
            @Override
            public boolean apply(Enum e)
            {
                return e.ordinal() == dbValue;
            }
        });
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Enum<?> value) throws SQLException
    {
        stmt.setInt(index, value.ordinal());
    }

    @Override
    public Object defaultParseValue(String value)
    {
        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new ActiveObjectsConfigurationException("Could not parse '" + value + "' as an integer to match enum ordinal");
        }
    }
}