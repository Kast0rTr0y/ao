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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.ActiveObjectsException;
import net.java.ao.EntityManager;

import static net.java.ao.util.DateUtils.*;

/**
 * Type mapping between Java Date and JDBC timestamp.  The underlying SQL type name defaults to "DATETIME",
 * but may be overridden by some database providers.
 * @author Daniel Spiewak
 */
public final class TimestampDateType extends DatabaseType<Date>
{
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public TimestampDateType(String sqlTypeIdentifier)
    {
            super(Types.TIMESTAMP, sqlTypeIdentifier, Date.class);
    }

    public TimestampDateType()
    {
        this("DATETIME");
    }

    @Override
    public Object validate(Object o)
    {
        if (!(o instanceof Date))
        {
            throw new ActiveObjectsException(o + " is not of type " + Date.class.getName());
        }
        return checkAgainstMaxDate((Date) o);
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Date value) throws SQLException
    {
        stmt.setTimestamp(index, new Timestamp(value.getTime()));
    }

    @Override
    public Date pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Date> type, String field) throws SQLException
    {
        return res.getTimestamp(field);
    }

    @Override
    public Date defaultParseValue(String value)
    {
        try
        {
            return checkAgainstMaxDate(newDateFormat(DATE_FORMAT).parse(value));
        }
        catch (ParseException e)
        {
            throw new ActiveObjectsConfigurationException("Could not parse '" + value + "' as a valid date following " + DATE_FORMAT);
        }
    }

    @Override
    public String valueToString(Object value)
    {
        return newDateFormat(DATE_FORMAT).format((Date) value);
    }
}