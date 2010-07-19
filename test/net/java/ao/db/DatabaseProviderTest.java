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

import static net.java.ao.DatabaseProviders.getEmbeddedDerbyDatabaseProvider;
import static net.java.ao.DatabaseProviders.getHsqlDatabaseProvider;
import static net.java.ao.DatabaseProviders.getMsSqlDatabaseProvider;
import static net.java.ao.DatabaseProviders.getMySqlDatabaseProvider;
import static net.java.ao.DatabaseProviders.getOrableDatabaseProvider;
import static net.java.ao.DatabaseProviders.getPostgreSqlDatabaseProvider;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Types;
import java.util.Calendar;

import net.java.ao.DisposableDataSource;
import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.ClassType;
import net.java.ao.types.TypeManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.schema.Company;

/**
 * @author Daniel Spiewak
 */
public class DatabaseProviderTest {
	private static final PrintStream STDERR = System.err;
	private DisposableDataSource dataSource;
	@Before
	public void setUp() {
		System.setErr(new PrintStream(new OutputStream() {
			@Override
			public void write(int arg0) throws IOException {
			}
		}));

        dataSource = mock(DisposableDataSource.class);
	}

	@Test
	public void testRenderActionCreateTable() throws IOException {
		final DDLAction action = createActionCreateTable();

        testRenderAction("derby-create-table.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-create-table.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-create-table.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-create-table.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-create-table.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-create-table.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }

    @Test
	public void testRenderActionDropTable() throws IOException {
		final DDLAction action = createActionDropTable();
		final String[] ddl = {"DROP TABLE person"};

        testRenderAction(ddl, action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction(ddl, action, getHsqlDatabaseProvider(dataSource));
        testRenderAction(ddl, action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction(ddl, action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-drop-table.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction(ddl, action, getPostgreSqlDatabaseProvider(dataSource));
    }

    @Test
	public void testRenderActionAddColumn() throws IOException {
		final DDLAction action = createActionAddColumn();

        testRenderAction("derby-add-column.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-add-column.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-add-column.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-add-column.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-add-column.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-add-column.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionAlterColumn() throws IOException {
		final DDLAction action = createActionAlterColumn();

        testRenderAction(new String[0], action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-alter-column.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-alter-column.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-alter-column.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-alter-column.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-alter-column.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionDropColumn() throws IOException {
		final DDLAction action = createActionDropColumn();

        testRenderAction(new String[0], action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-drop-column.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-drop-column.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-drop-column.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-drop-column.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-drop-column.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionCreateIndex() throws IOException {
		final DDLAction action = createActionCreateIndex();

        testRenderAction("derby-create-index.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-create-index.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-create-index.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-create-index.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-create-index.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-create-index.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionDropIndex() throws IOException {
		final DDLAction action = createActionDropIndex();

        testRenderAction("derby-drop-index.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-drop-index.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-drop-index.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-drop-index.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-drop-index.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-drop-index.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionAddKey() throws IOException {
		final DDLAction action = createActionAddKey();

        testRenderAction("derby-add-key.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-add-key.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-add-key.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-add-key.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-add-key.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-add-key.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@Test
	public void testRenderActionDropKey() throws IOException {
		final DDLAction action = createActionDropKey();

        testRenderAction("derby-drop-key.sql", action, getEmbeddedDerbyDatabaseProvider(dataSource));
        testRenderAction("hsqldb-drop-key.sql", action, getHsqlDatabaseProvider(dataSource));
        testRenderAction("sqlserver-drop-key.sql", action, getMsSqlDatabaseProvider(dataSource));
        testRenderAction("mysql-drop-key.sql", action, getMySqlDatabaseProvider(dataSource));
        testRenderAction("oracle-drop-key.sql", action, getOrableDatabaseProvider(dataSource));
        testRenderAction("postgres-drop-key.sql", action, getPostgreSqlDatabaseProvider(dataSource));
    }
	
	@After
	public void tearDown() {
        dataSource = null;
        System.setErr(STDERR);
	}
	
	private DDLAction createActionCreateTable() {
		TypeManager tm = TypeManager.getInstance();
		tm.addType(new ClassType());
		
		DDLTable table = new DDLTable();
		table.setName("person");
		
		DDLField[] fields = new DDLField[10];
		table.setFields(fields);
		
		fields[0] = new DDLField();
		fields[0].setName("id");
		fields[0].setType(tm.getType(int.class));
		fields[0].setAutoIncrement(true);
		fields[0].setPrimaryKey(true);
		fields[0].setNotNull(true);
		
		fields[1] = new DDLField();
		fields[1].setName("firstName");
		fields[1].setType(tm.getType(String.class));
		fields[1].setNotNull(true);
		
		fields[2] = new DDLField();
		fields[2].setName("lastName");
		fields[2].setType(tm.getType(Types.CLOB));
		
		fields[3] = new DDLField();
		fields[3].setName("age");
		fields[3].setType(tm.getType(int.class));
		fields[3].setPrecision(12);
		
		fields[4] = new DDLField();
		fields[4].setName("url");
		fields[4].setType(tm.getType(URL.class));
		fields[4].setUnique(true);
		
		fields[5] = new DDLField();
		fields[5].setName("favoriteClass");
		fields[5].setType(tm.getType(Class.class));
		
		fields[6] = new DDLField();
		fields[6].setName("height");
		fields[6].setType(tm.getType(double.class));
		fields[6].setPrecision(32);
		fields[6].setScale(6);
		fields[6].setDefaultValue(62.3);
		
		fields[7] = new DDLField();
		fields[7].setName("companyID");
		fields[7].setType(tm.getType(Company.class));
		
		fields[8] = new DDLField();
		fields[8].setName("cool");
		fields[8].setType(tm.getType(boolean.class));
		fields[8].setDefaultValue(true);
		
		fields[9] = new DDLField();
		fields[9].setName("created");
		fields[9].setType(tm.getType(Calendar.class));
		fields[9].setDefaultValue(DatabaseFunction.CURRENT_TIMESTAMP);
		fields[9].setOnUpdate(DatabaseFunction.CURRENT_TIMESTAMP);
		
		DDLForeignKey[] keys = new DDLForeignKey[1];
		table.setForeignKeys(keys);
		
		keys[0] = new DDLForeignKey();
		keys[0].setDomesticTable("person");
		keys[0].setField("companyID");
		keys[0].setForeignField("id");
		keys[0].setTable("company");
		
		DDLAction back = new DDLAction(DDLActionType.CREATE);
		back.setTable(table);
		
		return back;
	}
	
	private DDLAction createActionDropTable() {
		DDLAction back = new DDLAction(DDLActionType.DROP);
		
		DDLTable table = new DDLTable();
		table.setName("person");
		back.setTable(table);
		
		DDLField idField = new DDLField();
		idField.setName("id");
		idField.setType(TypeManager.getInstance().getType(int.class));
		idField.setAutoIncrement(true);
		idField.setNotNull(true);
		idField.setPrimaryKey(true);
		
		DDLField nameField = new DDLField();
		nameField.setName("name");
		nameField.setType(TypeManager.getInstance().getType(String.class));
		
		table.setFields(new DDLField[] {idField, nameField});
		
		return back;
	}
	
	private DDLAction createActionAddColumn() {
		DDLTable table = new DDLTable();
		table.setName("company");
		
		DDLField field = new DDLField();
		field.setName("name");
		field.setType(TypeManager.getInstance().getType(String.class));
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
		oldField.setType(TypeManager.getInstance().getType(int.class));
		oldField.setNotNull(false);
		table.setFields(new DDLField[] {oldField});
		
		DDLField field = new DDLField();
		field.setName("name");
		field.setType(TypeManager.getInstance().getType(String.class));
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
		field.setType(TypeManager.getInstance().getType(String.class));
		field.setNotNull(true);
		
		DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_COLUMN);
		back.setField(field);
		back.setTable(table);
		
		return back;
	}
	
	private DDLAction createActionAddKey() {
		DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_KEY);
		
		DDLForeignKey key = new DDLForeignKey();
		key.setDomesticTable("person");
		key.setField("companyID");
		key.setForeignField("id");
		key.setTable("company");
		back.setKey(key);
		
		return back;
	}
	
	private DDLAction createActionDropKey() {
		DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_KEY);
		
		DDLForeignKey key = new DDLForeignKey();
		key.setDomesticTable("person");
		key.setField("companyID");
		key.setForeignField("id");
		key.setTable("company");
		back.setKey(key);
		
		return back;
	}
	
	private DDLAction createActionCreateIndex() {
		DDLAction back = new DDLAction(DDLActionType.CREATE_INDEX);
		
		DDLIndex index = new DDLIndex();
		index.setField("companyID");
		index.setTable("person");
		index.setType(TypeManager.getInstance().getType(Types.VARCHAR));
		back.setIndex(index);
		
		return back;
	}
	
	private DDLAction createActionDropIndex() {
		DDLAction back = new DDLAction(DDLActionType.DROP_INDEX);
		
		DDLIndex index = new DDLIndex();
		index.setField("companyID");
		index.setTable("person");
		index.setType(TypeManager.getInstance().getType(Types.VARCHAR));
		back.setIndex(index);
		
		return back;
	}

    private void testRenderAction(String expectedSqlFile, DDLAction action, DatabaseProvider databaseProvider) throws IOException
    {
        testRenderAction(readStatements(expectedSqlFile), action, databaseProvider);
    }

    private void testRenderAction(String[] expectedSql, DDLAction action, DatabaseProvider databaseProvider)
    {
        assertArrayEquals(expectedSql, databaseProvider.renderAction(action));
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
