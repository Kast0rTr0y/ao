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

import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Spiewak
 */
public class DatabaseTypeTest
{
    @Test
	public void testDefaultParseValue() throws MalformedURLException {
		assertEquals(123, new IntegerType().defaultParseValue("123").intValue());
		assertEquals(123.456d, new DoubleType().defaultParseValue("123.456"), Double.MIN_VALUE);
		assertEquals(123.456f, new FloatType().defaultParseValue("123.456"), Float.MIN_VALUE);
		assertEquals("My test value", new VarcharType().defaultParseValue("My test value"));
		assertEquals(new URL("http://www.google.com"), new URLType().defaultParseValue("http://www.google.com"));
		assertEquals(false, new BooleanType().defaultParseValue("false"));
		assertEquals(String.class, new ClassType().defaultParseValue("java.lang.String"));
		assertEquals((short) 123, new TinyIntType().defaultParseValue("123").shortValue());
		assertEquals('c', new CharType().defaultParseValue("c").charValue());

		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Date date = new Date(0L);

		assertEquals(date, new TimestampDateType().defaultParseValue(dateFormatter.format(date)));
	}

	@Test
	public void testValueToString() throws MalformedURLException {
		assertEquals("123", new IntegerType().valueToString(123));
		assertEquals("123.456", new DoubleType().valueToString(123.456));
		assertEquals("123.456", new FloatType().valueToString(123.456));
		assertEquals("My test value", new VarcharType().valueToString("My test value"));
		assertEquals("http://www.google.com", new URLType().valueToString(new URL("http://www.google.com")));
		assertEquals("false", new BooleanType().valueToString(false));
		assertEquals("java.lang.String", new ClassType().valueToString(String.class));
		assertEquals("123", new TinyIntType().valueToString(123));
		assertEquals("c", new CharType().valueToString('c'));


		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = new Date(0L);

		assertEquals(dateFormatter.format(date), new TimestampDateType().valueToString(date));
	}
}
