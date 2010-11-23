package net.java.ao.schema.helper;

import net.java.ao.DatabaseProvider;
import net.java.ao.Query;
import net.java.ao.SchemaConfiguration;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.newHashMap;
import static net.java.ao.sql.SqlUtils.*;

public class DatabaseMetaDataReaderImpl implements DatabaseMetaDataReader
{
    private final DatabaseProvider databaseProvider;
    private final SchemaConfiguration schemaConfiguration;

    public DatabaseMetaDataReaderImpl(DatabaseProvider databaseProvider, SchemaConfiguration schemaConfiguration)
    {
        this.databaseProvider = databaseProvider;
        this.schemaConfiguration = schemaConfiguration;
    }

    public Iterable<String> getTableNames(DatabaseMetaData metaData)
    {
        ResultSet rs = null;
        try
        {
            rs = databaseProvider.getTables(metaData.getConnection());

            final List<String> tableNames = newLinkedList();
            while (rs.next())
            {
                final String tableName = rs.getString("TABLE_NAME");
                if (schemaConfiguration.shouldManageTable(tableName, databaseProvider.isCaseSensetive()))
                {
                    tableNames.add(tableName);
                }
            }
            return tableNames;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(rs);
        }
    }

    public Iterable<? extends Field> getFields(DatabaseMetaData databaseMetaData, String tableName)
    {
        final TypeManager manager = TypeManager.getInstance();
        final Map<String, FieldImpl> fields = newHashMap();

        try
        {
            final ResultSetMetaData rsmd = getResultSetMetaData(databaseMetaData, tableName);
            for (int i = 1; i < rsmd.getColumnCount() + 1; i++)
            {
                final String fieldName = rsmd.getColumnName(i);
                final DatabaseType<?> databaseType = manager.getType(rsmd.getColumnType(i));
                final int precision = getFieldPrecision(rsmd, i);
                final int scale = getScale(rsmd, i);
                final boolean autoIncrement = rsmd.isAutoIncrement(i);
                final boolean notNull = isNotNull(rsmd, i);

                fields.put(fieldName, newField(fieldName, databaseType, precision, scale, autoIncrement, notNull));
            }

            ResultSet rs = null;
            try
            {
                rs = databaseMetaData.getColumns(null, null, tableName, null);
                while (rs.next())
                {
                    FieldImpl current = fields.get(rs.getString("COLUMN_NAME"));
                    current.setDefaultValue(databaseProvider.parseValue(current.getDatabaseType().getType(), rs.getString("COLUMN_DEF")));
                    current.setNotNull(rs.getString("IS_NULLABLE").equals("NO"));
                }
            }
            finally
            {
                closeQuietly(rs);
            }
            try
            {
                rs = databaseMetaData.getPrimaryKeys(null, null, tableName);
                while (rs.next())
                {
                    fields.get(rs.getString("COLUMN_NAME")).setPrimaryKey(true);
                }
            }
            finally
            {
                closeQuietly(rs);
            }
            return fields.values();
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private FieldImpl newField(String fieldName, DatabaseType<?> databaseType, int precision, int scale, boolean autoIncrement, boolean notNull)
    {
        return new FieldImpl(fieldName, databaseType, precision, scale, autoIncrement, notNull);
    }

    private boolean isNotNull(ResultSetMetaData resultSetMetaData, int fieldIndex) throws SQLException
    {
        return resultSetMetaData.isNullable(fieldIndex) == ResultSetMetaData.columnNoNulls;
    }

    private int getScale(ResultSetMetaData rsmd, int fieldIndex) throws SQLException
    {
        final int scale = rsmd.getScale(fieldIndex);
        return scale <= 0 ? -1 : scale;
    }

    private int getFieldPrecision(ResultSetMetaData resultSetMetaData, int fieldIndex) throws SQLException
    {
        int precision = resultSetMetaData.getPrecision(fieldIndex);

        // HSQL reports this for VARCHAR
        if (precision == Integer.MAX_VALUE && Types.VARCHAR == resultSetMetaData.getColumnType(fieldIndex))
        {
            precision = resultSetMetaData.getColumnDisplaySize(fieldIndex);
        }

        return precision <= 0 ? -1 : precision;
    }

    private ResultSetMetaData getResultSetMetaData(DatabaseMetaData metaData, String tableName) throws SQLException
    {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try
        {
            final Query query = Query.select("*").from(tableName).limit(1);

            stmt = metaData.getConnection().prepareStatement(databaseProvider.renderQuery(query, null, false));
            databaseProvider.setQueryStatementProperties(stmt, query);
            rs = stmt.executeQuery();

            return rs.getMetaData();
        }
        finally
        {
            closeQuietly(rs);
            closeQuietly(stmt);
        }
    }

    public Iterable<ForeignKey> getForeignKeys(DatabaseMetaData metaData, String tableName)
    {
        ResultSet resultSet = null;
        try
        {
            final List<ForeignKey> keys = newLinkedList();
            resultSet = getImportedKeys(metaData, tableName);
            while (resultSet.next())
            {
                keys.add(newForeignKey(resultSet, tableName));
            }
            return keys;
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            closeQuietly(resultSet);
        }
    }

    private ResultSet getImportedKeys(DatabaseMetaData metaData, String tableName) throws SQLException
    {
        return metaData.getImportedKeys(null, null, tableName);
    }

    private ForeignKey newForeignKey(ResultSet rs, String localTableName) throws SQLException
    {
        final String localFieldName = rs.getString("FKCOLUMN_NAME");
        final String foreignFieldName = rs.getString("PKCOLUMN_NAME");
        final String foreignTableName = rs.getString("PKTABLE_NAME");

        return new ForeignKeyImpl(localTableName, localFieldName, foreignTableName, foreignFieldName);
    }

    private static final class FieldImpl implements Field
    {
        private final String name;
        private final DatabaseType<?> databaseType;
        private final int precision, scale;
        private final boolean autoIncrement;
        private boolean notNull;
        private Object defaultValue;
        private boolean primaryKey;

        public FieldImpl(String name, DatabaseType<?> databaseType, int precision, int scale, boolean autoIncrement, boolean notNull)
        {
            this.name = name;
            this.databaseType = databaseType;
            this.precision = precision;
            this.scale = scale;
            this.autoIncrement = autoIncrement;
            this.notNull = notNull;
        }

        public String getName()
        {
            return name;
        }

        public DatabaseType<?> getDatabaseType()
        {
            return databaseType;
        }

        public int getPrecision()
        {
            return precision;
        }

        public int getScale()
        {
            return scale;
        }

        public boolean isAutoIncrement()
        {
            return autoIncrement;
        }

        public boolean isNotNull()
        {
            return notNull;
        }

        public void setNotNull(boolean notNull)
        {
            this.notNull = notNull;
        }

        public Object getDefaultValue()
        {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        public boolean isPrimaryKey()
        {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey)
        {
            this.primaryKey = primaryKey;
        }
    }

    private static final class ForeignKeyImpl implements ForeignKey
    {
        private final String localTableName, localFieldName, foreignTableName, foreignFieldName;

        public ForeignKeyImpl(String localTableName, String localFieldName, String foreignTableName, String foreignFieldName)
        {
            this.localTableName = localTableName;
            this.localFieldName = localFieldName;
            this.foreignTableName = foreignTableName;
            this.foreignFieldName = foreignFieldName;
        }

        public String getLocalTableName()
        {
            return localTableName;
        }

        public String getLocalFieldName()
        {
            return localFieldName;
        }

        public String getForeignTableName()
        {
            return foreignTableName;
        }

        public String getForeignFieldName()
        {
            return foreignFieldName;
        }
    }
}
