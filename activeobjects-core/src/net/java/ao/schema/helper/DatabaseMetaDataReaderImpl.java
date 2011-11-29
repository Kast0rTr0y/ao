package net.java.ao.schema.helper;

import net.java.ao.DatabaseProvider;
import net.java.ao.Query;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.NameConverters;
import net.java.ao.sql.AbstractCloseableResultSetMetaData;
import net.java.ao.sql.CloseableResultSetMetaData;
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
import java.util.Set;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static net.java.ao.sql.SqlUtils.*;

public class DatabaseMetaDataReaderImpl implements DatabaseMetaDataReader
{
    private final DatabaseProvider databaseProvider;
    private final NameConverters nameConverters;
    private final SchemaConfiguration schemaConfiguration;

    public DatabaseMetaDataReaderImpl(DatabaseProvider databaseProvider, NameConverters nameConverters, SchemaConfiguration schemaConfiguration)
    {
        this.databaseProvider = databaseProvider;
        this.nameConverters = nameConverters;
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
        final List<String> sequenceNames = getSequenceNames(databaseMetaData);
        final Set<String> uniqueFields = getUniqueFields(databaseMetaData, tableName);

        final Map<String, FieldImpl> fields = newHashMap();

        CloseableResultSetMetaData rsmd = null;
        try
        {
            rsmd = getResultSetMetaData(databaseMetaData, tableName);
            for (int i = 1; i < rsmd.getColumnCount() + 1; i++)
            {
                final String fieldName = rsmd.getColumnName(i);
                final DatabaseType<?> databaseType = manager.getType(rsmd.getColumnType(i));
                final int precision = getFieldPrecision(rsmd, i);
                final int scale = getScale(rsmd, i);
                final boolean autoIncrement = isAutoIncrement(rsmd, i, sequenceNames, tableName, fieldName);
                final boolean notNull = isNotNull(rsmd, i);
                final boolean isUnique = isUnique(uniqueFields, fieldName);

                fields.put(fieldName, newField(fieldName, databaseType, precision, scale, autoIncrement, notNull, isUnique));
            }

            ResultSet rs = null;
            try
            {
                rs = databaseMetaData.getColumns(null, null, tableName, null);
                while (rs.next())
                {
                    final String columnName = rs.getString("COLUMN_NAME");
                    final FieldImpl current = fields.get(columnName);
                    if (current == null)
                    {
                        throw new IllegalStateException("Could not find column '" + columnName + "' in previously parsed query!");
                    }
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
                    final String fieldName = rs.getString("COLUMN_NAME");
                    final FieldImpl field = fields.get(fieldName);
                    field.setPrimaryKey(true);
                    field.setUnique(false); // MSSQL server 2005 tells us that the primary key is a unique key, this isn't what we want, we want real 'added' by hand unique keys.
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
            throw new RuntimeException("Could not read fields for table " + tableName, e);
        }
        finally
        {
            if (rsmd != null)
            {
                rsmd.close();
            }
        }
    }

    @Override
    public Iterable<? extends Index> getIndexes(DatabaseMetaData databaseMetaData, String tableName)
    {
        ResultSet resultSet = null;
        try
        {
            final List<Index> indexes = newLinkedList();
            resultSet = databaseProvider.getIndexes(databaseMetaData.getConnection(), tableName);
            while (resultSet.next())
            {
                boolean nonUnique = resultSet.getBoolean("NON_UNIQUE");
                if (nonUnique)
                {
                    indexes.add(newIndex(resultSet, tableName));
                }
            }
            return indexes;
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


    private Set<String> getUniqueFields(DatabaseMetaData metaData, String tableName)
    {
        ResultSet rs = null;
        try
        {
            rs = databaseProvider.getIndexes(metaData.getConnection(), tableName);

            final Set<String> fields = newHashSet();
            while (rs.next())
            {
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                if (!nonUnique)
                {
                    fields.add(rs.getString("COLUMN_NAME"));
                }
            }
            return fields;
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Could not get unique fields for table '" + tableName + "'", e);
        }
        finally
        {
            closeQuietly(rs);
        }
    }

    private List<String> getSequenceNames(DatabaseMetaData metaData)
    {
        ResultSet rs = null;
        try
        {
            rs = databaseProvider.getSequences(metaData.getConnection());

            final List<String> sequenceNames = newLinkedList();
            while (rs.next())
            {
                sequenceNames.add(databaseProvider.processID(rs.getString("TABLE_NAME")));
            }
            return sequenceNames;
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

    private boolean isAutoIncrement(ResultSetMetaData rsmd, int i, List<String> sequenceNames, String tableName, String fieldName) throws SQLException
    {
        boolean autoIncrement = rsmd.isAutoIncrement(i);
        if (!autoIncrement)
        {
            autoIncrement = isUsingSequence(sequenceNames, tableName, fieldName);
        }
        return autoIncrement;
    }

    private boolean isUsingSequence(List<String> sequenceNames, String tableName, String fieldName)
    {
        return sequenceNames.contains(databaseProvider.processID(nameConverters.getSequenceNameConverter().getName(tableName, fieldName)));
    }

    private boolean isUnique(Set<String> uniqueFields, String fieldName) throws SQLException
    {
        return uniqueFields.contains(fieldName);
    }

    private FieldImpl newField(String fieldName, DatabaseType<?> databaseType, int precision, int scale, boolean autoIncrement, boolean notNull, boolean isUnique)
    {
        return new FieldImpl(fieldName, databaseType, precision, scale, autoIncrement, notNull, isUnique);
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

    private CloseableResultSetMetaData getResultSetMetaData(DatabaseMetaData metaData, String tableName) throws SQLException
    {
        final Query query = Query.select("*").from(tableName).limit(1);
        final PreparedStatement stmt = metaData.getConnection().prepareStatement(databaseProvider.renderQuery(query, null, false));

        databaseProvider.setQueryStatementProperties(stmt, query);
        final ResultSet rs = stmt.executeQuery();

        return new AbstractCloseableResultSetMetaData(rs.getMetaData())
        {
            public void close()
            {
                closeQuietly(rs);
                closeQuietly(stmt);
            }
        };
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
        return databaseProvider.getImportedKeys(metaData.getConnection(), tableName);
    }

    private ForeignKey newForeignKey(ResultSet rs, String localTableName) throws SQLException
    {
        final String localFieldName = rs.getString("FKCOLUMN_NAME");
        final String foreignFieldName = rs.getString("PKCOLUMN_NAME");
        final String foreignTableName = rs.getString("PKTABLE_NAME");

        return new ForeignKeyImpl(localTableName, localFieldName, foreignTableName, foreignFieldName);
    }

    private Index newIndex(ResultSet rs, String tableName) throws SQLException
    {
        final String fieldName = rs.getString("COLUMN_NAME");
        return new IndexImpl(tableName, fieldName);
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
        private boolean isUnique;

        public FieldImpl(String name, DatabaseType<?> databaseType, int precision, int scale, boolean autoIncrement, boolean notNull, boolean isUnique)
        {
            this.name = name;
            this.databaseType = databaseType;
            this.precision = precision;
            this.scale = scale;
            this.autoIncrement = autoIncrement;
            this.notNull = notNull;
            this.isUnique = isUnique;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public DatabaseType<?> getDatabaseType()
        {
            return databaseType;
        }

        @Override
        public int getPrecision()
        {
            return precision;
        }

        @Override
        public int getScale()
        {
            return scale;
        }

        @Override
        public boolean isAutoIncrement()
        {
            return autoIncrement;
        }

        @Override
        public boolean isNotNull()
        {
            return notNull;
        }

        public void setNotNull(boolean notNull)
        {
            this.notNull = notNull;
        }

        @Override
        public Object getDefaultValue()
        {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue)
        {
            this.defaultValue = defaultValue;
        }

        @Override
        public boolean isPrimaryKey()
        {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey)
        {
            this.primaryKey = primaryKey;
        }

        @Override
        public boolean isUnique()
        {
            return isUnique;
        }

        public void setUnique(boolean unique)
        {
            isUnique = unique;
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

    private static final class IndexImpl implements Index
    {
        private final String tableName, fieldName;

        public IndexImpl(String tableName, String fieldName)
        {
            this.tableName = tableName;
            this.fieldName = fieldName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getFieldName()
        {
            return fieldName;
        }
    }
}
