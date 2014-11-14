package net.java.ao.db;

import com.google.common.collect.Lists;
import net.java.ao.DisposableDataSource;
import net.java.ao.Query;
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
import test.schema.Company;
import test.schema.Person;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;

import static net.java.ao.db.SqlActionStatementMatcher.hasStatement;
import static net.java.ao.db.SqlActionStatementMatcher.hasStatementMatching;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
        when(connMetaData.getIdentifierQuoteString()).thenReturn("'");

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

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' ADD CONSTRAINT U_da_table_teh_field UNIQUE ('teh_field')"));
        assertThat("should only add the constraint", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRemovedUniqueConstraint() throws Exception
    {
        oldField.setUnique(true);
        newField.setUnique(false);

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' DROP CONSTRAINT U_da_table_teh_field"));
        assertThat("should only drop the constraint", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRenamedColumn() throws Exception
    {
        newField.setName("the_field");

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' RENAME COLUMN 'teh_field' TO 'the_field'"));
        assertThat("should only rename the column", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRenamedColumnAndRemovedUniqueConstraint() throws Exception
    {
        oldField.setUnique(true);
        newField.setUnique(false);
        newField.setName("the_field");

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' DROP CONSTRAINT U_da_table_teh_field"));
        assertThat(ddl.get(1), hasStatement("ALTER TABLE public.'da_table' RENAME COLUMN 'teh_field' TO 'the_field'"));
        assertThat(ddl, hasSize(2));
    }

    @Test
    public void alterTableChangeColumnHandlesAddedRemovedConstraintAndAddedDefault() throws Exception
    {
        oldField.setUnique(true);
        newField.setUnique(false);
        newField.setDefaultValue("abc");

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' DROP CONSTRAINT U_da_table_teh_field"));
        assertThat(ddl.get(1), hasStatement("ALTER TABLE public.'da_table' ALTER COLUMN 'teh_field' SET DEFAULT 'abc'"));
        assertThat(ddl, hasSize(2));
    }

    @Test
    public void alterTableChangeColumnHandlesChangedType() throws Exception
    {
        oldField.setType(TypeManager.postgres().getType(Integer.class));
        newField.setType(TypeManager.postgres().getType(Long.class));

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' ALTER COLUMN 'teh_field' TYPE BIGINT"));
        assertThat("should only change the column type", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesAddedDefaultValue() throws Exception
    {
        oldField.setDefaultValue(null);
        newField.setDefaultValue("empty");

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' ALTER COLUMN 'teh_field' SET DEFAULT 'empty'"));
        assertThat("should only add the default value", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRemovedDefaultValue() throws Exception
    {
        oldField.setDefaultValue("empty");
        newField.setDefaultValue(null);

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' ALTER COLUMN 'teh_field' DROP DEFAULT"));
        assertThat("should only drop the default value", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesAddedNotNullConstraint() throws Exception
    {
        oldField.setNotNull(false);
        newField.setNotNull(true);

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatementMatching(("ALTER TABLE .* ALTER COLUMN .* SET NOT NULL")));
        assertThat("should only add the constraint", ddl, hasSize(1));
    }

    @Test
    public void alterTableChangeColumnHandlesRemovedNotNullConstraint() throws Exception
    {
        oldField.setNotNull(true);
        newField.setNotNull(false);

        List<SQLAction> ddl = renderAlterTableChangeColumn();
        assertThat(ddl.get(0), hasStatement("ALTER TABLE public.'da_table' ALTER COLUMN 'teh_field' DROP NOT NULL"));
        assertThat("should only drop the constraint", ddl, hasSize(1));
    }

    @Test
    public void joinQuery()
    {
        Query query = Query.select("name").from(Person.class).join(Company.class).where("cool = ?", true);
        String rendered = provider.renderQuery(query, nameConverters.getTableNameConverter(), false);
        assertThat(rendered, is("SELECT 'person'.'name' FROM public.'person' JOIN public.'company' WHERE 'cool' = ?"));
    }

    private DDLField createField()
    {
        DDLField field = new DDLField();
        field.setName("teh_field");
        field.setType(TypeManager.postgres().getType(String.class));

        return field;
    }

    private List<SQLAction> renderAlterTableChangeColumn()
    {
        return Lists.newArrayList(provider.renderAlterTableChangeColumn(nameConverters, table, oldField, newField));
    }
}
