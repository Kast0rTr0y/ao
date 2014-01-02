package net.java.ao.db;

import com.google.common.collect.Lists;
import net.java.ao.DisposableDataSource;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.types.TypeManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith (MockitoJUnitRunner.class)
public class PostgreSQLDatabaseProviderTest
{
    @Mock DisposableDataSource datasource;
    @Mock Connection conn;
    @Mock DatabaseMetaData connMetaData;

    NameConverters nameConverters;
    DDLTable table;

    PostgreSQLDatabaseProvider provider;
    private DDLField oldField;
    private DDLField newField;

    @Before
    public void setUp() throws Exception
    {
        when(datasource.getConnection()).thenReturn(conn);
        when(conn.getMetaData()).thenReturn(connMetaData);
        when(connMetaData.getIdentifierQuoteString()).thenReturn("\"");

        table = new DDLTable();
        table.setName("da_table");

        oldField = createField();
        newField = createField();

        nameConverters = new TestNameConverters();
        provider = new PostgreSQLDatabaseProvider(datasource);
    }

    @Test
    public void alterTableChangeColumnHandlesAddedUniqueConstraint() throws Exception
    {
        oldField.setUnique(false);
        newField.setUnique(true);

        List<SQLAction> ddl = Lists.newArrayList(provider.renderAlterTableChangeColumn(nameConverters, table, oldField, newField));
        assertThat(ddl.get(0).getStatement(), equalTo("ALTER TABLE public.\"da_table\" ADD CONSTRAINT U_da_table_teh_field UNIQUE (\"teh_field\")"));
        assertThat("DDL should only add the constraint and nothing more", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRemovedUniqueConstraint() throws Exception
    {
        oldField.setUnique(true);
        newField.setUnique(false);

        List<SQLAction> ddl = Lists.newArrayList(provider.renderAlterTableChangeColumn(nameConverters, table, oldField, newField));
        assertThat(ddl.get(0).getStatement(), equalTo("ALTER TABLE public.\"da_table\" DROP CONSTRAINT U_da_table_teh_field"));
        assertThat("DDL should only drop the constraint and nothing more", ddl, hasSize(1));
    }

    private DDLField createField()
    {
        DDLField field = new DDLField();
        field.setName("teh_field");
        field.setType(TypeManager.postgres().getType(String.class));

        return field;
    }
}
