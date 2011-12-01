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

import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import net.java.ao.EntityManager;

/**
 * @author Daniel Spiewak
 */
public class EnumType extends DatabaseType<Enum<?>> {

	public EnumType(String sqlTypeIdentifier)
	{
		super(Types.INTEGER, 4, sqlTypeIdentifier, Enum.class);
	}

	public EnumType()
	{
	    this("INTEGER");
	}
	
    @Override
    public boolean isDefaultForSqlType()
    {
        return false;
    }
	
	@Override
	public Enum<?> pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Enum<?>> type, 
			String field) throws SQLException {
		Enum<?>[] values = null;
		int dbValue = res.getInt(field);
		
		try {
			values = (Enum<?>[]) type.getMethod("values").invoke(null);
		} catch (IllegalArgumentException e) {
		} catch (SecurityException e) {
		} catch (IllegalAccessException e) {
		} catch (InvocationTargetException e) {
		} catch (NoSuchMethodException e) {
		}
		
		assert values != null;
		for (Enum<?> value : values) {
			if (dbValue == value.ordinal()) {
				return value;
			}
		}
		
		return null;
	}
	
	@Override
	public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, Enum<?> value) throws SQLException {
		stmt.setInt(index, value.ordinal());
	}

	@Override
	public Object defaultParseValue(String value) {
		return Integer.parseInt(value);
	}
}
