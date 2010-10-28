package net.java.ao.schema;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import net.java.ao.DatabaseProvider;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.DDLValue;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.sql.ActiveObjectSqlException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.*;
import static net.java.ao.sql.SqlUtils.closeQuietly;

public class BackupRestoreImpl implements BackupRestore
{
    public List<DDLAction> backup(DatabaseProvider provider, SchemaConfiguration schemaConfiguration)
    {
        try
        {
            final DDLTable[] tablesWithForeignKeys = SchemaReader.readSchema(provider, schemaConfiguration, true);
            final DDLTable[] tablesWithoutForeignKeys = SchemaReader.readSchema(provider, schemaConfiguration, false);

            final List<DDLAction> allActions = Lists.newLinkedList();

            // adding drop for all tables
            allActions.addAll(getDropActions(provider, tablesWithForeignKeys));

            // schema creation
            allActions.addAll(getSchemaActions(provider, tablesWithoutForeignKeys));

            allActions.addAll(getDataActions(provider, tablesWithoutForeignKeys));

            allActions.addAll(getForeignKeysActions(tablesWithForeignKeys));

            return allActions;
        }
        catch (SQLException e)
        {
            throw new ActiveObjectSqlException(e);
        }
    }

    private List<? extends DDLAction> getForeignKeysActions(DDLTable[] tablesWithForeignKeys)
    {
        final List<DDLAction> foreignKeyActions = newLinkedList();
        for (DDLTable table : tablesWithForeignKeys)
        {
            for (DDLForeignKey ddlForeignKey : table.getForeignKeys())
            {
                final DDLAction a = new DDLAction(DDLActionType.ALTER_ADD_KEY);
                a.setKey(ddlForeignKey);
                foreignKeyActions.add(a);
            }
        }
        return foreignKeyActions;
    }

    private List<? extends DDLAction> getDataActions(DatabaseProvider provider, DDLTable[] tables) throws SQLException
    {
        final List<DDLAction> data = Lists.newLinkedList();
        for (DDLTable table : tables)
        {
            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;
            try
            {
                conn = provider.getConnection();
                stmt = conn.createStatement();
                resultSet = stmt.executeQuery("select * from " + table.getName());
                while (resultSet.next())
                {
                    final DDLAction ddlAction = new DDLAction(DDLActionType.INSERT);
                    ddlAction.setTable(table);

                    List<DDLValue> values = Lists.newArrayList();
                    for (DDLField field : table.getFields())
                    {
                        final DDLValue val = new DDLValue();
                        val.setField(field);
                        val.setValue(resultSet.getObject(field.getName()));
                        values.add(val);
                    }

                    ddlAction.setValues(values.toArray(new DDLValue[values.size()]));
                    data.add(ddlAction);
                }
            }
            finally
            {
                closeQuietly(resultSet);
                closeQuietly(stmt);
                closeQuietly(conn);
            }
        }
        return data;
    }

    private List<DDLAction> getDropActions(DatabaseProvider provider, DDLTable[] tables)
    {
        return Arrays.asList(SchemaReader.sortTopologically(SchemaReader.diffSchema(new DDLTable[]{}, tables, provider.isCaseSensetive())));
    }

    private List<DDLAction> getSchemaActions(DatabaseProvider provider, DDLTable[] tables)
    {
        return Arrays.asList(SchemaReader.sortTopologically(SchemaReader.diffSchema(tables, new DDLTable[]{}, provider.isCaseSensetive())));
    }

    public void restore(List<DDLAction> actions, DatabaseProvider provider)
    {
        Connection conn = null;
        Statement stmt = null;
        try
        {
            conn = provider.getConnection();
            stmt = conn.createStatement();
            for (DDLAction action : actions)
            {
                restore(action, stmt, provider);
            }
        }
        catch (SQLException e)
        {
            throw new ActiveObjectSqlException(e);
        }
        finally
        {
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    private void restore(DDLAction action, final Statement stmt, DatabaseProvider provider) throws SQLException
    {
        final Iterable<String> sqlStatements = filter(newArrayList(provider.renderAction(action)), new NotEmptyStringPredicate());
        final ExecuteSqlFunction executeSqlFunction = new ExecuteSqlFunction(stmt, ignoreExceptionForAction(action));
        for (String sql : sqlStatements)
        {
            executeSqlFunction.apply(sql);
        }
    }

    private boolean ignoreExceptionForAction(DDLAction action)
    {
        return action.getActionType().equals(DDLActionType.DROP)
                || action.getActionType().equals(DDLActionType.DROP_INDEX)
                || action.getActionType().equals(DDLActionType.ALTER_DROP_KEY)
                || action.getActionType().equals(DDLActionType.ALTER_DROP_COLUMN);
    }

    private static void executeSql(Statement stmt, String sql, boolean ignoreException)
    {
        try
        {
            stmt.executeUpdate(sql);
        }
        catch (SQLException e)
        {
            if (!ignoreException)
            {
                throw new ActiveObjectSqlException(e);
            }
            else
            {

            }
        }
    }

    private static class NotEmptyStringPredicate implements Predicate<String>
    {
        public boolean apply(String input)
        {
            return input != null && input.length() > 0;
        }
    }

    private static class ExecuteSqlFunction implements Function<String, Void>
    {
        private final Statement stmt;
        private final boolean ignoreException;

        public ExecuteSqlFunction(Statement stmt, boolean ignoreException)
        {
            this.stmt = stmt;
            this.ignoreException = ignoreException;
        }

        public Void apply(String sql)
        {
            executeSql(stmt, sql, ignoreException);
            return null;
        }
    }
}
