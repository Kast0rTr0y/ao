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

import java.net.URI;
import java.net.URL;
import java.sql.Types;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import static net.java.ao.types.LogicalTypes.booleanType;

import static net.java.ao.types.TypeQualifiers.UNLIMITED_LENGTH;

import static net.java.ao.types.LogicalTypes.dateType;

import static net.java.ao.types.LogicalTypes.floatType;

import static net.java.ao.types.LogicalTypes.doubleType;

import static net.java.ao.types.SchemaProperties.schemaType;

import static net.java.ao.types.LogicalTypes.longType;

import static net.java.ao.types.LogicalTypes.integerType;

import static net.java.ao.types.LogicalTypes.stringType;

import test.schema.Person;

import static net.java.ao.types.TypeQualifiers.qualifiers;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Spiewak
 */
public final class TypeManagerTest
{
    private static final TypeQualifiers PLAIN = qualifiers();
    private static final TypeQualifiers PRECISION_100 = qualifiers().precision(100);
    private static final TypeQualifiers LENGTH_127 = qualifiers().stringLength(127);
    private static final TypeQualifiers LENGTH_STRING_DEFAULT = qualifiers().stringLength(StringType.DEFAULT_LENGTH);
    private static final TypeQualifiers LENGTH_URL_DEFAULT = qualifiers().stringLength(TypeQualifiers.MAX_STRING_LENGTH);
    private static final TypeQualifiers UNLIMITED = qualifiers().stringLength(UNLIMITED_LENGTH);
    
    private TypeManager typeManager;
    
    @Before
    public final void setUp()
    {
        typeManager = new TypeManager.Builder()
            .addMapping(booleanType(), schemaType("BOOLEAN"))
            .addMapping(integerType(), schemaType("INTEGER"))
            .addMapping(longType(), schemaType("BIGINT").precisionAllowed(true), PRECISION_100)
            .addMapping(doubleType(), schemaType("DOUBLE"))
            .addMapping(floatType(), schemaType("FLOAT"))
            .addMapping(dateType(), schemaType("DATETIME"))
            .addStringTypes("VARCHAR", "TEXT")
            .build();
    }

    @Test
	public void testGetTypeClass()
    {
        verifyType(typeManager.getType(boolean.class), BooleanType.class, "BOOLEAN", PLAIN);
        verifyType(typeManager.getType(Boolean.class), BooleanType.class, "BOOLEAN", PLAIN);
        verifyType(typeManager.getType(int.class), IntegerType.class, "INTEGER", PLAIN);
        verifyType(typeManager.getType(Integer.class), IntegerType.class, "INTEGER", PLAIN);
        verifyType(typeManager.getType(long.class), LongType.class, "BIGINT", PRECISION_100);
        verifyType(typeManager.getType(Long.class), LongType.class, "BIGINT", PRECISION_100);
        verifyType(typeManager.getType(double.class), DoubleType.class, "DOUBLE", PLAIN);
        verifyType(typeManager.getType(Double.class), DoubleType.class, "DOUBLE", PLAIN);
        verifyType(typeManager.getType(float.class), FloatType.class, "FLOAT", PLAIN);
        verifyType(typeManager.getType(Float.class), FloatType.class, "FLOAT", PLAIN);
        verifyType(typeManager.getType(String.class), StringType.class, "VARCHAR", LENGTH_STRING_DEFAULT);
        verifyType(typeManager.getType(String.class, LENGTH_127), StringType.class, "VARCHAR", LENGTH_127);
        verifyType(typeManager.getType(String.class, UNLIMITED), StringType.class, "TEXT", UNLIMITED);
        verifyType(typeManager.getType(URI.class), URIType.class, "VARCHAR", LENGTH_URL_DEFAULT);
        verifyType(typeManager.getType(URI.class, LENGTH_127), URIType.class, "VARCHAR", LENGTH_127);
        verifyType(typeManager.getType(URI.class, UNLIMITED), URIType.class, "TEXT", UNLIMITED);
        verifyType(typeManager.getType(URL.class), URLType.class, "VARCHAR", LENGTH_URL_DEFAULT);
        verifyType(typeManager.getType(URL.class, LENGTH_127), URLType.class, "VARCHAR", LENGTH_127);
        verifyType(typeManager.getType(URL.class, UNLIMITED), URLType.class, "TEXT", UNLIMITED);
        verifyType(typeManager.getType(Person.class), EntityType.class, "INTEGER", PLAIN);
	}

    @Test
    public void testGetTypeInt()
    {
        verifyType(typeManager.getTypeFromSchema(Types.VARCHAR, LENGTH_127), StringType.class, "VARCHAR", LENGTH_127);
        verifyType(typeManager.getTypeFromSchema(Types.CLOB, PLAIN), StringType.class, "TEXT", UNLIMITED);
        verifyType(typeManager.getTypeFromSchema(Types.INTEGER, PLAIN), IntegerType.class, "INTEGER", PLAIN);
        verifyType(typeManager.getTypeFromSchema(Types.NUMERIC, PRECISION_100), LongType.class, "BIGINT", PRECISION_100);
    }

    private void verifyType(TypeInfo<?> typeInfo, Class<?> logicalTypeShouldBe, String sqlTypeShouldBe, TypeQualifiers qualsShouldBe)
    {
        assertEquals(logicalTypeShouldBe, typeInfo.getLogicalType().getClass());
        assertEquals(sqlTypeShouldBe, typeInfo.getSchemaProperties().getSqlTypeName());
        assertEquals(qualsShouldBe, typeInfo.getQualifiers());
    }
}
