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
package net.java.ao.schema;

import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonSuit;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

/**
 * @author Daniel Spiewak
 */
@Data(DatabaseProcessor.class)
public class SchemaGeneratorTest extends ActiveObjectsIntegrationTest
{
    @Ignore("currently failing in mysql; Eli will fix after merge")
    @SuppressWarnings("null")
    @Test
    public void testParseDDL()
    {
        String[] expectedFields = {
                getFieldName(Person.class, "getID"),
                getFieldName(Person.class, "getFirstName"),
                getFieldName(Person.class, "getLastName"),
                getFieldName(Person.class, "getProfession"),
                getFieldName(Person.class, "getAge"),
                getFieldName(Person.class, "getURL"),
                getFieldName(Person.class, "getCompany"),
                getFieldName(Person.class, "getImage"),
                getFieldName(Person.class, "isActive"),
                getFieldName(Person.class, "getModified")};

        String[] expectedIndexes = {getFieldName(Person.class, "getAge"), getFieldName(Person.class, "getCompany")};

        TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        DDLTable[] parsedTables = SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(), PersonSuit.class, Pen.class);

        assertEquals(6, parsedTables.length);

        DDLTable personDDL = null;
        for (DDLTable table : parsedTables)
        {
            if (table.getName().equals(tableNameConverter.getName(Person.class)))
            {
                personDDL = table;
                break;
            }
        }

        assertNotNull(personDDL);
        assertEquals(expectedFields.length, personDDL.getFields().length);
        assertEquals(1, personDDL.getForeignKeys().length);

        for (DDLField field : personDDL.getFields())
        {
            boolean found = false;
            for (String expectedField : expectedFields)
            {
                if (expectedField.equals(field.getName()))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Field " + field.getName() + " was unexpected");
            }
        }

        DDLField urlField = null;
        DDLField ageField = null;
        DDLField lastNameField = null;
        DDLField idField = null;
        DDLField cidField = null;

        for (DDLField field : personDDL.getFields())
        {
            if (field.getName().equals(getFieldName(Person.class, "getURL")))
            {
                urlField = field;
            }
            else if (field.getName().equals(getFieldName(Person.class, "getAge")))
            {
                ageField = field;
            }
            else if (field.getName().equals(getFieldName(Person.class, "getLastName")))
            {
                lastNameField = field;
            }
            else if (field.getName().equals(getFieldName(Person.class, "getID")))
            {
                idField = field;
            }
            else if (field.getName().equals(getFieldName(Person.class, "getCompany")))
            {
                cidField = field;
            }
        }

        assertEquals(Types.VARCHAR, urlField.getType().getType());

        assertFalse(urlField.isAutoIncrement());
        assertTrue(urlField.isNotNull());
        assertFalse(urlField.isPrimaryKey());
        assertTrue(urlField.isUnique());

        assertNotNull(urlField.getDefaultValue());

        assertEquals(Types.INTEGER, ageField.getType().getType());

        assertFalse(ageField.isAutoIncrement());
        assertFalse(ageField.isNotNull());
        assertFalse(ageField.isPrimaryKey());
        assertFalse(ageField.isUnique());

        assertEquals("VARCHAR(127)", lastNameField.getType().getSqlTypeIdentifier());

        assertFalse(lastNameField.isAutoIncrement());
        assertFalse(lastNameField.isNotNull());
        assertFalse(lastNameField.isPrimaryKey());
        assertFalse(lastNameField.isUnique());

        assertEquals(Types.INTEGER, idField.getType().getType());

        assertTrue(idField.isAutoIncrement());
        assertTrue(idField.isNotNull());
        assertTrue(idField.isPrimaryKey());
        assertFalse(idField.isUnique());

        assertNull(idField.getDefaultValue());

        assertEquals(Types.BIGINT, cidField.getType().getType());

        assertFalse(cidField.isAutoIncrement());
        assertFalse(cidField.isNotNull());
        assertFalse(cidField.isPrimaryKey());
        assertFalse(cidField.isUnique());

        assertNull(cidField.getDefaultValue());

        DDLForeignKey cidKey = null;
        for (DDLForeignKey key : personDDL.getForeignKeys())
        {
            if (key.getField().equals(getFieldName(Person.class, "getCompany")))
            {
                cidKey = key;
                break;
            }
        }

        assertNotNull(cidKey);

        assertEquals(tableNameConverter.getName(Person.class), cidKey.getDomesticTable());
        assertEquals(getFieldName(Person.class, "getCompany"), cidKey.getField());
        assertEquals(getFieldName(Company.class, "getCompanyID"), cidKey.getForeignField());
        assertEquals(tableNameConverter.getName(Company.class), cidKey.getTable());

        assertEquals(expectedIndexes.length, personDDL.getIndexes().length);
        for (DDLIndex index : personDDL.getIndexes())
        {
            boolean found = false;
            for (String expectedIndex : expectedIndexes)
            {
                if (expectedIndex.equals(index.getField()))
                {
                    assertEquals(tableNameConverter.getName(Person.class), index.getTable());

                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Index for field " + index.getField() + " was unexpected");
            }
        }
    }
}
