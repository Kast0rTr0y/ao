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
import java.sql.Types;

import net.java.ao.EntityManager;

/**
 * @author Daniel Spiewak
 */
public class TinyIntType extends DatabaseType<Short> {

	public TinyIntType(String sqlTypeIdentifier)
	{
		super(Types.TINYINT, -1, sqlTypeIdentifier, short.class, Short.class);
	}
	
	public TinyIntType()
	{
	    this("TINYINT");
	}
	
	@Override
	public Short pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Short> type, String field) throws SQLException {
		return res.getShort(field);
	}

	@Override
	public Short defaultParseValue(String value) {
		return Short.parseShort(value);
	}
}
