package net.java.ao.schema.helper;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.ao.DatabaseProvider;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.NameConverters;
import net.java.ao.sql.CloseableResultSetMetaData;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import net.java.ao.types.TypeQualifiers;
import net.java.ao.util.StringUtils;

import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static net.java.ao.types.TypeQualifiers.qualifiers;

public class DatabaseMetaDataReaderImpl implements DatabaseMetaDataReader
{
    private static final Pattern STRING_VALUE = Pattern.compile("\"(.*)\"");

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
                final String tableName = parseStringValue(rs, "TABLE_NAME");
                if (schemaConfiguration.shouldManageTable(tableName, databaseProvider.isCaseSensitive()))
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
        final TypeManager manager = databaseProvider.getTypeManager();
        final List<String> sequenceNames = getSequenceNames(databaseMetaData);
        final Set<String> uniqueFields = getUniqueFields(databaseMetaData, tableName);

        final Map<String, FieldImpl> fields = newHashMap();

        CloseableResultSetMetaData rsmd = null;
        try
        {
            ResultSet rs = null;
            try
            {
                rs = databaseMetaData.getColumns(null, null, tableName, null);
                while (rs.next())
                {
                    final String fieldName = parseStringValue(rs, "COLUMN_NAME");

                    final TypeQualifiers qualifiers = getTypeQualifiers(rs);
                    final int jdbcType = rs.getInt("DATA_TYPE");
                    final TypeInfo<?> databaseType = manager.getTypeFromSchema(jdbcType, qualifiers);
                    boolean autoIncrement = /*parseStringValue(rs, "IS_AUTOINCREMENT").equals("YES") ||*/ isUsingSequence(sequenceNames, tableName, fieldName);
                    final boolean notNull = parseStringValue(rs, "IS_NULLABLE").equals("NO");
                    final boolean isUnique = isUnique(uniqueFields, fieldName);

                    final FieldImpl fieldImpl = newField(fieldName, databaseType, jdbcType, autoIncrement, notNull, isUnique);

                    final Object defaultValue = databaseProvider.parseValue(jdbcType, parseStringValue(rs, "COLUMN_DEF"));
                    fieldImpl.setDefaultValue(defaultValue);

                    fields.put(fieldName, fieldImpl);
                }
            }
            finally
            {
                closeQuietly(rs);
            }
            try
            {
                rs = databaseMetaData.getPrimaryKeys(null, databaseProvider.getSchema(), tableName);
                while (rs.next())
                {
                    final String fieldName = parseStringValue(rs, "COLUMN_NAME");
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
                    fields.add(parseStringValue(rs, "COLUMN_NAME"));
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
                sequenceNames.add(databaseProvider.processID(parseStringValue(rs, "TABLE_NAME")));
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

    private boolean isUsingSequence(List<String> sequenceNames, String tableName, String fieldName)
    {
        return sequenceNames.contains(databaseProvider.processID(nameConverters.getSequenceNameConverter().getName(tableName, fieldName)));
    }

    private boolean isUnique(Set<String> uniqueFields, String fieldName) throws SQLException
    {
        return uniqueFields.contains(fieldName);
    }

    private FieldImpl newField(String fieldName, TypeInfo<?> databaseType, int jdbcType, boolean autoIncrement, boolean notNull, boolean isUnique)
    {
        return new FieldImpl(fieldName, databaseType, jdbcType, autoIncrement, notNull, isUnique);
    }

    private boolean isNotNull(ResultSetMetaData resultSetMetaData, int fieldIndex) throws SQLException
    {
        return resultSetMetaData.isNullable(fieldIndex) == ResultSetMetaData.columnNoNulls;
    }

    private TypeQualifiers getTypeQualifiers(ResultSet rs) throws SQLException
    {
        TypeQualifiers ret = qualifiers();

        if (rs.getInt("DATA_TYPE") == Types.VARCHAR)
        {
            final int length = rs.getInt("COLUMN_SIZE");
            if (length > 0) {
                ret = ret.stringLength(length);
            }
        }
        else
        {
            final int precision = rs.getInt("COLUMN_SIZE");
            final int scale = rs.getInt("DECIMAL_DIGITS");
            if (precision > 0)
            {
                ret = ret.precision(precision);
            }
            if (scale > 0) {
                ret = ret.scale(scale);
            }
        }

        return ret;
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
        final String localFieldName = parseStringValue(rs, "FKCOLUMN_NAME");
        final String foreignFieldName = parseStringValue(rs, "PKCOLUMN_NAME");
        final String foreignTableName = parseStringValue(rs, "PKTABLE_NAME");

        return new ForeignKeyImpl(localTableName, localFieldName, foreignTableName, foreignFieldName);
    }

    private Index newIndex(ResultSet rs, String tableName) throws SQLException
    {
        return new IndexImpl(tableName, parseStringValue(rs, "COLUMN_NAME"), parseStringValue(rs, "INDEX_NAME"));
    }

    private String parseStringValue(ResultSet rs, String columnName) throws SQLException
    {
        final String value = rs.getString(columnName);
        if (StringUtils.isBlank(value))
        {
            return value;
        }
        final Matcher m = STRING_VALUE.matcher(value);
        return m.find() ? m.group(1) : value;
    }

    private static final class FieldImpl implements Field
    {
        private final String name;
        private final TypeInfo<?> databaseType;
        private final int jdbcType;
        private final boolean autoIncrement;
        private boolean notNull;
        private Object defaultValue;
        private boolean primaryKey;
        private boolean isUnique;

        public FieldImpl(String name, TypeInfo<?> databaseType, int jdbcType, boolean autoIncrement, boolean notNull, boolean isUnique)
        {
            this.name = name;
            this.databaseType = databaseType;
            this.jdbcType = jdbcType;
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
        public TypeInfo<?> getDatabaseType()
        {
            return databaseType;
        }

        @Override
        public int getJdbcType()
        {
            return jdbcType;
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
        private final String tableName, fieldName, indexName;

        public IndexImpl(String tableName, String fieldName, String indexName)
        {
            this.tableName = tableName;
            this.fieldName = fieldName;
            this.indexName = indexName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getFieldName()
        {
            return fieldName;
        }

        @Override
        public String getIndexName()
        {
            return indexName;
        }
    }
}
