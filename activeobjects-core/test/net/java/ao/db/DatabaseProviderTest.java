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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
import net.java.ao.schema.ddl.SQLAction;
import test.schema.Company;

import static com.google.common.collect.Lists.newArrayList;
import static net.java.ao.types.TypeQualifiers.UNLIMITED_LENGTH;
import static net.java.ao.types.TypeQualifiers.qualifiers;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public abstract class DatabaseProviderTest
{
    @Mock
    private NameConverters nameConverters;

    @Before
    public final void setUp()
    {
        when(nameConverters.getSequenceNameConverter()).thenReturn(new DefaultSequenceNameConverter());
        when(nameConverters.getTriggerNameConverter()).thenReturn(new DefaultTriggerNameConverter());
        when(nameConverters.getIndexNameConverter()).thenReturn(new DefaultIndexNameConverter());
        when(nameConverters.getUniqueNameConverter()).thenReturn(new DefaultUniqueNameConverter());
    }

    protected abstract String getDatabase();

    protected abstract DatabaseProvider getDatabaseProvider();

    @Test
    public final void testRenderActionCreateTable() throws IOException
    {
        testRenderAction("create-table.sql", createActionCreateTable, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionDropTable() throws IOException
    {
        testRenderAction("drop-table.sql", createActionDropTable, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionAddColumn() throws IOException
    {
        testRenderAction("add-column.sql", createActionAddColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterColumn() throws IOException
    {
        testRenderAction("alter-column.sql", createActionAlterColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionDropColumn() throws IOException
    {
        testRenderAction("drop-column.sql", createActionDropColumn, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionCreateIndex() throws IOException
    {
        testRenderAction("create-index.sql", createActionCreateIndex, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionDropIndex() throws IOException
    {
        testRenderAction("drop-index.sql", createActionDropIndex, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionAddKey() throws IOException
    {
        testRenderAction("add-key.sql", createActionAddKey, getDatabaseProvider());
    }

    @Test
    public final void testRenderActionDropKey() throws IOException
    {
        testRenderAction("drop-key.sql", createActionDropKey, getDatabaseProvider());
    }

    @Test
    public final void testProcessWhereClause()
    {
        final String where = "field1 = 2 and field2 like %er";
        assertEquals(getExpectedWhereClause(), getDatabaseProvider().processWhereClause(where));
    }

    protected String getExpectedWhereClause()
    {
        return "field1 = 2 and field2 like %er";
    }

    private Function<DatabaseProvider, DDLAction> createActionCreateTable = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            final DDLTable table = new DDLTable();
            table.setName("person");

            final List<DDLField> fields = newArrayList(
                    newIdField(db),
                    newFirstNameField(db),
                    newLastNameField(db),
                    newAgeField(db),
                    newUrlField(db),
                    newHeightField(db),
                    newCompanyField(db),
                    newCoolField(db),
                    newModifiedField(db),
                    newWeightField(db),
                    newTypeOfPersonField(db));
            DDLField[] fieldsArray = new DDLField[fields.size()];
            table.setFields(fieldsArray);
            fields.toArray(fieldsArray);

            final List<DDLForeignKey> keys = newArrayList(newForeignKey(db));
            table.setForeignKeys(keys.toArray(new DDLForeignKey[keys.size()]));

            DDLAction back = new DDLAction(DDLActionType.CREATE);
            back.setTable(table);

            return back;
        }
    };

    private DDLForeignKey newForeignKey(DatabaseProvider db)
    {
        DDLForeignKey fk = new DDLForeignKey();
        fk.setDomesticTable("person");
        fk.setField("companyID");
        fk.setForeignField("id");
        fk.setTable("company");
        return fk;
    }

    private DDLField newModifiedField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("modified");
        f.setType(db.getTypeManager().getType(Date.class));
        return f;
    }

    private DDLField newWeightField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("weight");
        f.setType(db.getTypeManager().getType(double.class));
        return f;
    }

    private DDLField newCoolField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("cool");
        f.setType(db.getTypeManager().getType(boolean.class));
        f.setDefaultValue(true);
        return f;
    }

    private DDLField newCompanyField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("companyID");
        f.setType(db.getTypeManager().getType(Company.class));
        return f;
    }

    private DDLField newHeightField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("height");
        f.setType(db.getTypeManager().getType(double.class));
        f.setDefaultValue(62.3);
        return f;
    }

    private DDLField newUrlField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("url");
        f.setType(db.getTypeManager().getType(URL.class));
        f.setUnique(true);
        return f;
    }

    private DDLField newAgeField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("age");
        f.setType(db.getTypeManager().getType(int.class));
        return f;
    }

    private DDLField newLastNameField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("lastName");
        f.setType(db.getTypeManager().getType(String.class, qualifiers().stringLength(UNLIMITED_LENGTH)));
        return f;
    }

    private DDLField newFirstNameField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("firstName");
        f.setType(db.getTypeManager().getType(String.class));
        f.setNotNull(true);
        return f;
    }

    private DDLField newIdField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("id");
        f.setType(db.getTypeManager().getType(int.class));
        f.setAutoIncrement(true);
        f.setPrimaryKey(true);
        f.setNotNull(true);
        return f;
    }

    private DDLField newTypeOfPersonField(DatabaseProvider db)
    {
        DDLField f = new DDLField();
        f.setName("typeOfPerson");
        f.setType(db.getTypeManager().getType(TypeOfPerson.class, qualifiers().stringLength(30)));
        return f;
    }

    private Function<DatabaseProvider, DDLAction> createActionDropTable = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLAction back = new DDLAction(DDLActionType.DROP);

            DDLTable table = new DDLTable();
            table.setName("person");
            back.setTable(table);

            DDLField idField = new DDLField();
            idField.setName("id");
            idField.setType(db.getTypeManager().getType(int.class));
            idField.setAutoIncrement(true);
            idField.setNotNull(true);
            idField.setPrimaryKey(true);

            DDLField nameField = new DDLField();
            nameField.setName("name");
            nameField.setType(db.getTypeManager().getType(String.class));

            table.setFields(new DDLField[]{idField, nameField});

            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionAddColumn = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField field = new DDLField();
            field.setName("name");
            field.setType(db.getTypeManager().getType(String.class));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_COLUMN);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };

    protected final Function<DatabaseProvider, DDLAction> createActionAlterColumn = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField oldField = new DDLField();
            oldField.setName("name");
            oldField.setType(db.getTypeManager().getType(int.class));
            oldField.setNotNull(false);
            table.setFields(new DDLField[]{oldField});

            DDLField field = new DDLField();
            field.setName("name");
            field.setType(db.getTypeManager().getType(String.class));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_CHANGE_COLUMN);
            back.setOldField(oldField);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };

    protected final Function<DatabaseProvider, DDLAction> createActionDropColumn = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLTable table = new DDLTable();
            table.setName("company");

            DDLField field = new DDLField();
            field.setName("name");
            field.setType(db.getTypeManager().getType(String.class));
            field.setNotNull(true);

            DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_COLUMN);
            back.setField(field);
            back.setTable(table);

            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionAddKey = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLAction back = new DDLAction(DDLActionType.ALTER_ADD_KEY);
            back.setKey(newForeignKey(db));
            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionDropKey = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLAction back = new DDLAction(DDLActionType.ALTER_DROP_KEY);
            back.setKey(newForeignKey(db));
            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionCreateIndex = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLAction back = new DDLAction(DDLActionType.CREATE_INDEX);

            DDLIndex index = new DDLIndex();
            index.setField("companyID");
            index.setTable("person");
            index.setType(db.getTypeManager().getType(String.class));
            back.setIndex(index);

            return back;
        }
    };

    private Function<DatabaseProvider, DDLAction> createActionDropIndex = new Function<DatabaseProvider, DDLAction>()
    {
        public DDLAction apply(DatabaseProvider db)
        {
            DDLAction back = new DDLAction(DDLActionType.DROP_INDEX);

            DDLIndex index = new DDLIndex();
            index.setField("companyID");
            index.setTable("person");
            index.setType(db.getTypeManager().getType(String.class));
            back.setIndex(index);

            return back;
        }
    };

    protected final void testRenderAction(String expectedSqlFile, Function<DatabaseProvider, DDLAction> action, DatabaseProvider databaseProvider) throws IOException
    {
        testRenderAction(readStatements(expectedSqlFile), action, databaseProvider);
    }

    protected final void testRenderAction(String[] expectedSql, Function<DatabaseProvider, DDLAction> action, DatabaseProvider databaseProvider)
    {
        ImmutableList.Builder<String> statements = ImmutableList.builder();
        for (SQLAction sql : databaseProvider.renderAction(nameConverters, action.apply(databaseProvider)))
        {
            statements.add(sql.getStatement());
        }
        if (expectedSql.length == 0)
        {
            assertThat(statements.build(), Matchers.<String>iterableWithSize(0));
        }
        else
        {
            assertThat(statements.build(), contains(expectedSql));
        }
    }

    private String[] readStatements(String resource) throws IOException
    {
        StringBuilder back = new StringBuilder();

        BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/net/java/ao/db/" + getDatabase() + "/" + resource)));
        String cur;
        while ((cur = reader.readLine()) != null)
        {
            back.append(cur).append('\n');
        }
        reader.close();

        back.setLength(back.length() - 1);

        String[] arr = back.toString().split("\n\n");
        for (int i = 0; i < arr.length; i++)
        {
            arr[i] = arr[i].trim();
        }

        return arr;
    }
    
    static enum TypeOfPerson
    {
        ME,
        EVERYONE_ELSE
    }
}
