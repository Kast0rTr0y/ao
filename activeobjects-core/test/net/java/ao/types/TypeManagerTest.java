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

import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.sql.Types;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import test.schema.Person;

/**
 * @author Daniel Spiewak
 */
public final class TypeManagerTest
{
    private TypeManager typeManager;

    @Before
    public final void setUp()
    {
        typeManager = new TypeManager.Builder().build();
    }

    @Test
	public void testGetTypeClass()
    {
		assertEquals(new VarcharType(), typeManager.getType(String.class));
		assertEquals(new IntegerType(), typeManager.getType(int.class));
		assertEquals(new IntegerType(), typeManager.getType(Integer.class));
		assertEquals(new DoubleType(), typeManager.getType(double.class));
		assertEquals(new TimestampDateType(), typeManager.getType(Date.class));
		assertEquals(new EntityType<Integer>(typeManager, Person.class), typeManager.getType(Person.class));
		assertEquals(new URLType(), typeManager.getType(URL.class));
	}

	@Test
	public void testGetTypeInt()
    {
		assertEquals(new VarcharType(), typeManager.getType(Types.VARCHAR));
		assertEquals(new IntegerType(), typeManager.getType(Types.INTEGER));
		assertEquals(new GenericType(Types.JAVA_OBJECT), typeManager.getType(Types.JAVA_OBJECT));

	}
}
