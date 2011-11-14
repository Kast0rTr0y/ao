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

import net.java.ao.Common;
import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonSuit;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;

import static org.junit.Assert.*;

@Data(DatabaseProcessor.class)
public final class SchemaReaderTest extends ActiveObjectsIntegrationTest
{
    @SuppressWarnings("null")
    @Test
    public void testReadSchema() throws SQLException
    {
        String[] expectedFields = {
                getFieldName(Person.class, "getID"),
                getFieldName(Person.class, "getFirstName"),
                getFieldName(Person.class, "getLastName"),
                getFieldName(Person.class, "getProfession"),
                getFieldName(Person.class, "getAge"),
                getFieldName(Person.class, "getURL"),
                getFieldName(Person.class, "getFavoriteClass"),
                getFieldName(Person.class, "getCompany"),
                getFieldName(Person.class, "getImage"),
                getFieldName(Person.class, "isActive"),
                getFieldName(Person.class, "getModified")};

        DDLTable[] parsedTables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), new DefaultSchemaConfiguration());

        assertEquals(22, parsedTables.length);

        DDLTable personDDL = null;
        for (DDLTable table : parsedTables)
        {
            if (table.getName().equalsIgnoreCase(getTableName(Person.class, false)))
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
                if (expectedField.equalsIgnoreCase(field.getName()))
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
        for (DDLField field : personDDL.getFields())
        {
            if (field.getName().equalsIgnoreCase("url"))
            {
                urlField = field;
                break;
            }
        }

        assertTrue(Common.fuzzyTypeCompare(Types.VARCHAR, urlField.getType().getType()));

        assertFalse(urlField.isAutoIncrement());
        assertNotNull(urlField.getDefaultValue());

        DDLField idField = null;
        for (DDLField field : personDDL.getFields())
        {
            if (field.getName().equalsIgnoreCase(getFieldName(Person.class, "getID")))
            {
                idField = field;
                break;
            }
        }

        assertTrue(Common.fuzzyTypeCompare(Types.INTEGER, idField.getType().getType()));

        assertTrue(idField.isAutoIncrement());
        assertNull(idField.getDefaultValue());

        DDLField cidField = null;
        for (DDLField field : personDDL.getFields())
        {
            if (field.getName().equalsIgnoreCase(getFieldName(Person.class, "getCompany")))
            {
                cidField = field;
                break;
            }
        }

        assertTrue(Common.fuzzyTypeCompare(Types.BIGINT, cidField.getType().getType()));

        assertFalse(cidField.isAutoIncrement());
        assertNull(cidField.getDefaultValue());

        DDLForeignKey cidKey = null;
        for (DDLForeignKey key : personDDL.getForeignKeys())
        {
            if (key.getField().equalsIgnoreCase(getFieldName(Person.class, "getCompany")))
            {
                cidKey = key;
                break;
            }
        }

        assertNotNull(cidKey);

        assertTrue(getTableName(Person.class, false).equalsIgnoreCase(cidKey.getDomesticTable()));
        assertTrue(getFieldName(Person.class, "getCompany").equalsIgnoreCase(cidKey.getField()));
        assertTrue(getFieldName(Company.class, "getCompanyID").equalsIgnoreCase(cidKey.getForeignField()));
        assertTrue(getTableName(Company.class, false).equalsIgnoreCase(cidKey.getTable()));
    }

    @Test
    public void testDiffSchema()
    {
        DDLTable[] ddl1 = SchemaGenerator.parseDDL(entityManager.getNameConverters(), PersonSuit.class, Pen.class);
        DDLTable[] ddl2 = SchemaGenerator.parseDDL(entityManager.getNameConverters(), PersonSuit.class, Pen.class);

        assertEquals(0, SchemaReader.diffSchema(ddl1, ddl2, true).length);
    }

    @Test
    public void testSortTopologically()
    {
        DDLTable table1 = new DDLTable();
        table1.setName("table1");

        DDLField table1Field1 = new DDLField();
        table1Field1.setName("field1");

        DDLField table1Field2 = new DDLField();
        table1Field2.setName("field2");

        DDLField[] table1Fields = {table1Field1, table1Field2};
        table1.setFields(table1Fields);

        DDLForeignKey table1Key1 = new DDLForeignKey();
        table1Key1.setDomesticTable("table1");
        table1Key1.setField("field1");
        table1Key1.setForeignField("field2");
        table1Key1.setTable("table2");

        table1.setForeignKeys(new DDLForeignKey[]{table1Key1});

        DDLTable table2 = new DDLTable();
        table2.setName("table2");

        DDLField table2Field1 = new DDLField();
        table2Field1.setName("field1");

        DDLField table2Field2 = new DDLField();
        table2Field2.setName("field2");

        DDLField[] table2Fields = {table2Field1, table2Field2};
        table2.setFields(table2Fields);

        DDLTable table3 = new DDLTable();
        table3.setName("table3");

        DDLField table3Field1 = new DDLField();
        table3Field1.setName("field1");

        DDLField table3Field2 = new DDLField();
        table3Field2.setName("field2");

        DDLField table3Field3 = new DDLField();
        table3Field3.setName("field3");

        DDLField[] table3Fields = {table3Field1, table3Field2, table3Field3};
        table3.setFields(table3Fields);

        // rendering
        DDLAction[] actions = SchemaReader.diffSchema(new DDLTable[]{table1, table2, table3}, new DDLTable[0], true);
        actions = SchemaReader.sortTopologically(actions);

        assertEquals(3, actions.length);
        assertNotSame(table1, actions[0].getTable());

        for (DDLAction action : actions)
        {
            assertEquals(DDLActionType.CREATE, action.getActionType());
        }

        if (actions[1].getTable().equals(table1))
        {
            assertEquals(table2, actions[0].getTable());
        }
        else if (actions[1].getTable().equals(table2))
        {
            assertEquals(table1, actions[2].getTable());
        }
    }

    @Test
    public void testAlterDropForeignKeyComesBeforeDropTable()
    {
        final String localTableName = "localTableName";
        final String foreignTableName = "foreignTableName";

        final DDLAction dropForeignKey = newDropForeignKey(localTableName, foreignTableName);
        final DDLAction dropLocalTable = newDropTable(localTableName);
        final DDLAction dropForeignTable = newDropTable(foreignTableName);

        final DDLAction[] ddlActions = SchemaReader.sortTopologically(new DDLAction[]{dropForeignTable, dropForeignKey, dropLocalTable});

        assertEquals(dropForeignKey, ddlActions[0]);
        // that's it we don't care in which order tables are dropped
    }

    private DDLAction newDropTable(String localTableName)
    {
        final DDLTable ddlTable = new DDLTable();
        ddlTable.setName(localTableName);

        final DDLAction dropTable = new DDLAction(DDLActionType.DROP);
        dropTable.setTable(ddlTable);
        return dropTable;
    }

    private DDLAction newDropForeignKey(String localTableName, String foreignTableName)
    {
        final DDLForeignKey foreignKey = new DDLForeignKey();
        foreignKey.setDomesticTable(localTableName);
        foreignKey.setTable(foreignTableName);

        final DDLAction dropForeignKey = new DDLAction(DDLActionType.ALTER_DROP_KEY);
        dropForeignKey.setKey(foreignKey);
        return dropForeignKey;
    }
}
