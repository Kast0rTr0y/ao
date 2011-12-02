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

import static net.java.ao.types.NumericTypeProperties.numericType;

/**
 * @author Daniel Spiewak
 */
public class FloatType extends AbstractNumericType<Float>
{
    public FloatType(NumericTypeProperties properties)
    {
        super(Types.FLOAT, properties, float.class, Float.class);
    }
    
	public FloatType()
	{
        this(numericType("FLOAT"));
	}
	
	@Override
	public Float pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends Float> type, String field) throws SQLException {
		return res.getFloat(field);
	}

	@Override
	public Float defaultParseValue(String value) {
		return Float.parseFloat(value);
	}
}
