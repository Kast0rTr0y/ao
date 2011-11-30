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
package net.java.ao.db;

import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.DefaultUniqueNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.TypeManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import test.schema.Company;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static net.java.ao.DatabaseProviders.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public final class DatabaseProviderTest
{
    private TypeManager typeManager;

    @Mock
    private NameConverters nameConverters;

    @Before
    public final void setUp()
    {
        typeManager = new TypeManager();

        when(nameConverters.getSequenceNameConverter()).thenReturn(new DefaultSequenceNameConverter());
        when(nameConverters.getTriggerNameConverter()).thenReturn(new DefaultTriggerNameConverter());
        when(nameConverters.getIndexNameConverter()).thenReturn(new DefaultIndexNameConverter());
        when(nameConverters.getUniqueNameConverter()).thenReturn(new DefaultUniqueNameConverter());
    }

    @Test
    public void testRenderActionCreateTable() throws IOException
    {
        final DDLAction action = createActionCreateTable();

        testRenderAction("derby-create-table.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-create-table.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-create-table.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-create-table.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-create-table.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-create-table.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionDropTable() throws IOException
    {
        final DDLAction action = createActionDropTable();
        final String[] ddl = {"DROP TABLE person"};

        testRenderAction(ddl, action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-drop-table.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-drop-table.sql", action, getMsSqlDatabaseProvider());
        testRenderAction(ddl, action, getMySqlDatabaseProvider());
        testRenderAction("oracle-drop-table.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-drop-table.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionAddColumn() throws IOException
    {
        final DDLAction action = createActionAddColumn();

        testRenderAction("derby-add-column.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-add-column.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-add-column.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-add-column.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-add-column.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-add-column.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterColumn() throws IOException
    {
        final DDLAction action = createActionAlterColumn();

        testRenderAction(new String[0], action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-alter-column.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-alter-column.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-alter-column.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-alter-column.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-alter-column.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionDropColumn() throws IOException
    {
        final DDLAction action = createActionDropColumn();

        testRenderAction(new String[0], action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-drop-column.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-drop-column.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-drop-column.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-drop-column.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-drop-column.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionCreateIndex() throws IOException
    {
        final DDLAction action = createActionCreateIndex();

        testRenderAction("derby-create-index.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-create-index.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-create-index.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-create-index.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-create-index.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-create-index.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionDropIndex() throws IOException
    {
        final DDLAction action = createActionDropIndex();

        testRenderAction("derby-drop-index.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-drop-index.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-drop-index.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-drop-index.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-drop-index.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-drop-index.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionAddKey() throws IOException
    {
        final DDLAction action = createActionAddKey();

        testRenderAction("derby-add-key.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-add-key.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-add-key.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-add-key.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-add-key.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-add-key.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testRenderActionDropKey() throws IOException
    {
        final DDLAction action = createActionDropKey();

        testRenderAction("derby-drop-key.sql", action, getEmbeddedDerbyDatabaseProvider());
        testRenderAction("hsqldb-drop-key.sql", action, getHsqlDatabaseProvider());
        testRenderAction("sqlserver-drop-key.sql", action, getMsSqlDatabaseProvider());
        testRenderAction("mysql-drop-key.sql", action, getMySqlDatabaseProvider());
        testRenderAction("oracle-drop-key.sql", action, getOracleDatabaseProvider());
        testRenderAction("postgres-drop-key.sql", action, getPostgreSqlDatabaseProvider());
    }

    @Test
    public void testProcessWhereClause()
    {
        final String where = "field1 = 2 and field2 like %er";
        assertEquals(where, getEmbeddedDerbyDatabaseProvider().processWhereClause(where));
        assertEquals(where, getHsqlDatabaseProvider().processWhereClause(where));
        assertEquals(where, getMsSqlDatabaseProvider().processWhereClause(where));
        assertEquals(where, getMySqlDatabaseProvider().processWhereClause(where));
        assertEquals(where, getOracleDatabaseProvider().processWhereClause(where));
        assertEquals("\"field1\" = 2 and \"field2\" like %er", getPostgreSqlDatabaseProvider().processWhereClause(where));
    }

    private DDLAction createActionCreateTable()
    {
        final DDLTable table = new DDLTable();
        table.setName("person");

        final List<DDLField> fields = newArrayList(
                newIdField(),
                newFirstNameField(),
                newLastNameField(),
                newAgeField(),
                newUrlField(),
                newHeightField(),
                newCompanyField(),
                newCoolField(),
                newModifiedField(),
                newWeightField());
        DDLField[] fieldsArray = new DDLField[fields.size()];
        table.setFields(fieldsArray);
        fields.toArray(fieldsArray);

        final List<DDLForeignKey> keys = newArrayList(newForeignKey());
        table.setForeignKeys(keys.toArray(new DDLForeignKey[keys.size()]));

        DDLAction back = new DDLAction(DDLActionType.CREATE);
        back.setTable(table);

        return back;
    }

    private DDLForeignKey newForeignKey()
    {
        DDLForeignKey fk = new DDLForeignKey();
        fk.setDomesticTable("person");
        fk.setField("companyID");
        fk.setForeignField("id");
        fk.setTable("company");
        return fk;
    }

    private DDLField newWeightField()
    {
        DDLField f =  new DDLField();
        f.setName("weight");
        f.setType(typeManager.getType(double.class));
        return f;
    }

    private DDLField newModifiedField()
    {
        DDLField f =  new DDLField();
        f.setName("modified");
        f.setType(typeManager.getType(Calendar.class));
        f.setDefaultValue(DatabaseFunction.CURRENT_TIMESTAMP);
        f.setOnUpdate(DatabaseFunction.CURRENT_TIMESTAMP);
        return f;
    }

    private DDLField newCoolField()
    {
        DDLField f =  new DDLField();
        f.setName("cool");
        f.setType(typeManager.getType(boolean.class));
        f.setDefaultValue(true);
        return f;
    }

    private DDLField newCompanyField()
    {
        DDLField f  = new DDLField();
        f.setName("companyID");
        f.setType(typeManager.getType(Company.class));
        return f;
    }

    private DDLField newHeightField()
    {
        DDLField f  = new DDLField();
        f.setName("height");
        f.setType(typeManager.getType(double.class));
        f.setPrecision(32);
        f.setScale(6);
        f.setDefaultValue(62.3);
        return f;
    }

    private DDLField newUrlField()
    {
        DDLField f = new DDLField();
        f.setName("url");
        f.setType(typeManager.getType(URL.class));
        f.setUnique(true);
        return f;
    }

    private DDLField newAgeField()
    {
        DDLField f = new DDLField();
        f.setName("age");
        f.setType(typeManager.getType(int.class));
        f.setPrecision(12);
        return f;
    }

    private DDLField newLastNameField()
    {
        DDLField f = new DDLField();
        f.setName("lastName");
        f.setType(typeManager.getType(Types.CLOB));
        return f;
    }

    private DDLField newFirstNameField()
    {
        DDLField f  = new DDLField();
        f.setName("firstName");
        f.setType(typeManager.getType(String.class));
        f.setNotNull(true);
        return f;
    }

    private DDLField newIdField()
    {
        DDLField f = new DDLField();
        f.setName("id");
        f.setType(typeManager.getType(int.class));
        f.setAutoIncrement(true);
        f.setPrimaryKey(true);
        f.setNotNull(true);
        return f;
    }

    private DDLAction createActionDropTable() {
		DDLAction back = new DDLAction(DDLActionType.DROP);
		
		DDLTable table = new DDLTable();
		table.setName("person");
		back.setTable(table);
		
		DDLField idField = new DDLField();
		idField.setName("id");
		idField.setType(typeManager.getType(int.class));
		idField.setAutoIncrement(true);
		idField.setNotNull(true);
		idField.setPrimaryKey(true);
		
		DDLField nameField = new DDLField();
		nameField.setName("name");
		nameField.setType(typeManager.getType(String.class));
		
		table.setFields(new DDLField[] {idField, nameField});
		
		return back;
	}
	
	private DDLAction createActionAddColumn() {
		DDLTable table = new DDLTable();
		table.setName("company");
		
		DDLField field = new DDLField();
		field.setName("name");
		field.setType(typeManager.getType(String.class));
		field.setNotNull(true);
		
		DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_COLUMN);
		back.setField(field);
		back.setTable(table);
		
		return back;
	}
	
	private DDLAction createActionAlterColumn() {
		DDLTable table = new DDLTable();
		table.setName("company");
		
		DDLField oldField = new DDLField();
		oldField.setName("name");
		oldField.setType(typeManager.getType(int.class));
		oldField.setNotNull(false);
		table.setFields(new DDLField[] {oldField});
		
		DDLField field = new DDLField();
		field.setName("name");
		field.setType(typeManager.getType(String.class));
		field.setNotNull(true);
		
		DDLAction back = new DDLAction(DDLActionType.ALTER_CHANGE_COLUMN);
		back.setOldField(oldField);
		back.setField(field);
		back.setTable(table);
		
		return back;
	}
	
	private DDLAction createActionDropColumn() {
		DDLTable table = new DDLTable();
		table.setName("company");
		
		DDLField field = new DDLField();
		field.setName("name");
		field.setType(typeManager.getType(String.class));
		field.setNotNull(true);
		
		DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_COLUMN);
		back.setField(field);
		back.setTable(table);
		
		return back;
	}
	
	private DDLAction createActionAddKey() {
		DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_KEY);
        back.setKey(newForeignKey());
		return back;
	}
	
	private DDLAction createActionDropKey() {
		DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_KEY);
        back.setKey(newForeignKey());
		return back;
	}
	
	private DDLAction createActionCreateIndex() {
		DDLAction back = new DDLAction(DDLActionType.CREATE_INDEX);
		
		DDLIndex index = new DDLIndex();
		index.setField("companyID");
		index.setTable("person");
		index.setType(typeManager.getType(Types.VARCHAR));
		back.setIndex(index);
		
		return back;
	}
	
	private DDLAction createActionDropIndex() {
		DDLAction back = new DDLAction(DDLActionType.DROP_INDEX);
		
		DDLIndex index = new DDLIndex();
		index.setField("companyID");
		index.setTable("person");
		index.setType(typeManager.getType(Types.VARCHAR));
		back.setIndex(index);
		
		return back;
	}

    private void testRenderAction(String expectedSqlFile, DDLAction action, DatabaseProvider databaseProvider) throws IOException
    {
        testRenderAction(readStatements(expectedSqlFile), action, databaseProvider);
    }

    private void testRenderAction(String[] expectedSql, DDLAction action, DatabaseProvider databaseProvider)
    {
        assertArrayEquals(expectedSql, databaseProvider.renderAction(nameConverters, action));
    }

	private String[] readStatements(String resource) throws IOException {
		StringBuilder back = new StringBuilder();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				getClass().getResourceAsStream("/net/java/ao/db/" + resource)));
		String cur;
		while ((cur = reader.readLine()) != null) {
			back.append(cur).append('\n');
		}
		reader.close();
		
		back.setLength(back.length() - 1);
		
		String[] arr = back.toString().split("\n\n");
		for (int i = 0; i < arr.length; i++) {
			arr[i] = arr[i].trim();
		}
		
		return arr;
	}
}
