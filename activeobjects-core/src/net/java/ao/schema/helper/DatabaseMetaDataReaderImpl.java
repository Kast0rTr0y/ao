package net.java.ao.schema.helper;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import net.java.ao.DatabaseProvider;
import net.java.ao.RawEntity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.NameConverters;
import net.java.ao.sql.AbstractCloseableResultSetMetaData;
import net.java.ao.sql.CloseableResultSetMetaData;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import net.java.ao.types.TypeQualifiers;
import net.java.ao.util.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static net.java.ao.types.TypeQualifiers.qualifiers;

public class DatabaseMetaDataReaderImpl implements DatabaseMetaDataReader {
    private static final Pattern STRING_VALUE = Pattern.compile("\"(.*)\"");

    private final DatabaseProvider databaseProvider;
    private final NameConverters nameConverters;
    private final SchemaConfiguration schemaConfiguration;

    public DatabaseMetaDataReaderImpl(DatabaseProvider databaseProvider, NameConverters nameConverters, SchemaConfiguration schemaConfiguration) {
        this.databaseProvider = databaseProvider;
        this.nameConverters = nameConverters;
        this.schemaConfiguration = schemaConfiguration;
    }

    public boolean isTablePresent(final DatabaseMetaData databaseMetaData, final Class<? extends RawEntity<?>> type) {
        final String entityTableName = nameConverters.getTableNameConverter().getName(type);

        final Iterable<String> tableNames = getTableNames(databaseMetaData);

        return StreamSupport.stream(tableNames.spliterator(), false)
                .anyMatch(tableName -> tableName.equalsIgnoreCase(entityTableName));
    }

    public Iterable<String> getTableNames(DatabaseMetaData metaData) {
        ResultSet rs = null;
        try {
            rs = databaseProvider.getTables(metaData.getConnection());

            final List<String> tableNames = newLinkedList();
            while (rs.next()) {
                final String tableName = parseStringValue(rs, "TABLE_NAME");
                if (schemaConfiguration.shouldManageTable(tableName, databaseProvider.isCaseSensitive())) {
                    tableNames.add(tableName);
                }
            }
            return tableNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(rs);
        }
    }

    public Iterable<? extends Field> getFields(DatabaseMetaData databaseMetaData, String tableName) {
        final TypeManager manager = databaseProvider.getTypeManager();
        final List<String> sequenceNames = getSequenceNames(databaseMetaData);
        final Set<String> uniqueFields = getUniqueFields(databaseMetaData, tableName);

        final Map<String, FieldImpl> fields = newHashMap();

        CloseableResultSetMetaData rsmd = null;
        try {
            rsmd = getResultSetMetaData(databaseMetaData, tableName);
            for (int i = 1; i < rsmd.getColumnCount() + 1; i++) {
                final String fieldName = rsmd.getColumnName(i);
                final TypeQualifiers qualifiers = getTypeQualifiers(rsmd, i);
                final int jdbcType = rsmd.getColumnType(i);
                final TypeInfo<?> databaseType = manager.getTypeFromSchema(jdbcType, qualifiers);
                if (databaseType == null) {
                    StringBuilder buf = new StringBuilder();
                    buf.append("TABLE: " + tableName + ": ");
                    for (int j = 1; j <= rsmd.getColumnCount(); j++) {
                        buf.append(rsmd.getColumnName(j)).append(" - ");
                    }
                    buf.append("can't find type " + jdbcType + " " + qualifiers + " in field " + fieldName);
                    throw new IllegalStateException(buf.toString());
                }
                final boolean autoIncrement = isAutoIncrement(rsmd, i, sequenceNames, tableName, fieldName);
                final boolean notNull = isNotNull(rsmd, i);
                final boolean isUnique = isUnique(uniqueFields, fieldName);

                fields.put(fieldName, newField(fieldName, databaseType, jdbcType, autoIncrement, notNull, isUnique));
            }

            ResultSet rs = null;
            try {
                rs = databaseMetaData.getColumns(null, null, tableName, null);
                while (rs.next()) {
                    final String columnName = parseStringValue(rs, "COLUMN_NAME");
                    final FieldImpl current = fields.get(columnName);
                    if (current == null) {
                        throw new IllegalStateException("Could not find column '" + columnName + "' in previously parsed query!");
                    }
                    current.setDefaultValue(databaseProvider.parseValue(current.getJdbcType(), parseStringValue(rs, "COLUMN_DEF")));
                    current.setNotNull(current.isNotNull() || parseStringValue(rs, "IS_NULLABLE").equals("NO"));
                }
            } finally {
                closeQuietly(rs);
            }
            try {
                rs = databaseMetaData.getPrimaryKeys(null, databaseProvider.getSchema(), tableName);
                while (rs.next()) {
                    final String fieldName = parseStringValue(rs, "COLUMN_NAME");
                    final FieldImpl field = fields.get(fieldName);
                    field.setPrimaryKey(true);
                    field.setUnique(false); // MSSQL server 2005 tells us that the primary key is a unique key, this isn't what we want, we want real 'added' by hand unique keys.
                }
            } finally {
                closeQuietly(rs);
            }
            return fields.values();
        } catch (SQLException e) {
            throw new RuntimeException("Could not read fields for table " + tableName, e);
        } finally {
            if (rsmd != null) {
                rsmd.close();
            }
        }
    }

    @Override
    public Iterable<? extends Index> getIndexes(DatabaseMetaData databaseMetaData, String tableName) {
        final ImmutableList.Builder<Index> indexes = ImmutableList.builder();
        ResultSet resultSet = null;
        try {
            final Multimap<String, String> fieldsByIndex = ArrayListMultimap.create();
            resultSet = databaseProvider.getIndexes(databaseMetaData.getConnection(), tableName);
            while (resultSet.next()) {
                boolean nonUnique = resultSet.getBoolean("NON_UNIQUE");
                if (nonUnique) {
                    fieldsByIndex.put(parseStringValue(resultSet, "INDEX_NAME"), parseStringValue(resultSet, "COLUMN_NAME"));
                }
            }

            for (String indexName : fieldsByIndex.keySet()) {
                Collection<String> fieldNames = fieldsByIndex.get(indexName);

                indexes.add(new IndexImpl(indexName, tableName, fieldNames));
            }

            return indexes.build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet);
        }
    }

    private Set<String> getUniqueFields(DatabaseMetaData metaData, String tableName) {
        ResultSet rs = null;
        try {
            rs = databaseProvider.getIndexes(metaData.getConnection(), tableName);

            final Set<String> fields = newHashSet();
            while (rs.next()) {
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                if (!nonUnique) {
                    fields.add(parseStringValue(rs, "COLUMN_NAME"));
                }
            }
            return fields;
        } catch (SQLException e) {
            throw new RuntimeException("Could not get unique fields for table '" + tableName + "'", e);
        } finally {
            closeQuietly(rs);
        }
    }

    private List<String> getSequenceNames(DatabaseMetaData metaData) {
        ResultSet rs = null;
        try {
            rs = databaseProvider.getSequences(metaData.getConnection());

            final List<String> sequenceNames = newLinkedList();
            while (rs.next()) {
                sequenceNames.add(databaseProvider.processID(parseStringValue(rs, "TABLE_NAME")));
            }
            return sequenceNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    closeQuietly(rs.getStatement());
                } catch (SQLException e) {
                    //ignored
                }
            }
            closeQuietly(rs);
        }
    }

    private boolean isAutoIncrement(ResultSetMetaData rsmd, int i, List<String> sequenceNames, String tableName, String fieldName) throws SQLException {
        boolean autoIncrement = rsmd.isAutoIncrement(i);
        if (!autoIncrement) {
            autoIncrement = isUsingSequence(sequenceNames, tableName, fieldName);
        }
        return autoIncrement;
    }

    private boolean isUsingSequence(List<String> sequenceNames, String tableName, String fieldName) {
        return sequenceNames.contains(databaseProvider.processID(nameConverters.getSequenceNameConverter().getName(tableName, fieldName)));
    }

    private boolean isUnique(Set<String> uniqueFields, String fieldName) throws SQLException {
        return uniqueFields.contains(fieldName);
    }

    private FieldImpl newField(String fieldName, TypeInfo<?> databaseType, int jdbcType, boolean autoIncrement, boolean notNull, boolean isUnique) {
        return new FieldImpl(fieldName, databaseType, jdbcType, autoIncrement, notNull, isUnique);
    }

    private boolean isNotNull(ResultSetMetaData resultSetMetaData, int fieldIndex) throws SQLException {
        return resultSetMetaData.isNullable(fieldIndex) == ResultSetMetaData.columnNoNulls;
    }

    private TypeQualifiers getTypeQualifiers(ResultSetMetaData rsmd, int fieldIndex) throws SQLException {
        TypeQualifiers ret = qualifiers();
        if (rsmd.getColumnType(fieldIndex) == Types.VARCHAR) {
            int length = rsmd.getColumnDisplaySize(fieldIndex);
            if (length > 0) {
                ret = ret.stringLength(length);
            }
        } else {
            int precision = rsmd.getPrecision(fieldIndex);
            int scale = rsmd.getScale(fieldIndex);
            if (precision > 0) {
                ret = ret.precision(precision);
            }
            if (scale > 0) {
                ret = ret.scale(scale);
            }
        }
        return ret;
    }

    private CloseableResultSetMetaData getResultSetMetaData(DatabaseMetaData metaData, String tableName) throws SQLException {
        final PreparedStatement stmt = metaData.getConnection().prepareStatement(databaseProvider.renderMetadataQuery(tableName));
        final ResultSet rs = stmt.executeQuery();

        return new AbstractCloseableResultSetMetaData(rs.getMetaData()) {
            public void close() {
                closeQuietly(rs);
                closeQuietly(stmt);
            }
        };
    }

    public Iterable<ForeignKey> getForeignKeys(DatabaseMetaData metaData, String tableName) {
        ResultSet resultSet = null;
        try {
            final List<ForeignKey> keys = newLinkedList();
            resultSet = getImportedKeys(metaData, tableName);
            while (resultSet.next()) {
                keys.add(newForeignKey(resultSet, tableName));
            }
            return keys;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(resultSet);
        }
    }

    private ResultSet getImportedKeys(DatabaseMetaData metaData, String tableName) throws SQLException {
        return databaseProvider.getImportedKeys(metaData.getConnection(), tableName);
    }

    private ForeignKey newForeignKey(ResultSet rs, String localTableName) throws SQLException {
        final String localFieldName = parseStringValue(rs, "FKCOLUMN_NAME");
        final String foreignFieldName = parseStringValue(rs, "PKCOLUMN_NAME");
        final String foreignTableName = parseStringValue(rs, "PKTABLE_NAME");

        return new ForeignKeyImpl(localTableName, localFieldName, foreignTableName, foreignFieldName);
    }

    private String parseStringValue(ResultSet rs, String columnName) throws SQLException {
        final String value = rs.getString(columnName);
        if (StringUtils.isBlank(value)) {
            return value;
        }
        final Matcher m = STRING_VALUE.matcher(value);
        return m.find() ? m.group(1) : value;
    }

    private static final class FieldImpl implements Field {
        private final String name;
        private final TypeInfo<?> databaseType;
        private final int jdbcType;
        private final boolean autoIncrement;
        private boolean notNull;
        private Object defaultValue;
        private boolean primaryKey;
        private boolean isUnique;

        public FieldImpl(String name, TypeInfo<?> databaseType, int jdbcType, boolean autoIncrement, boolean notNull, boolean isUnique) {
            this.name = name;
            this.databaseType = databaseType;
            this.jdbcType = jdbcType;
            this.autoIncrement = autoIncrement;
            this.notNull = notNull;
            this.isUnique = isUnique;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public TypeInfo<?> getDatabaseType() {
            return databaseType;
        }

        @Override
        public int getJdbcType() {
            return jdbcType;
        }

        @Override
        public boolean isAutoIncrement() {
            return autoIncrement;
        }

        @Override
        public boolean isNotNull() {
            return notNull;
        }

        public void setNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        @Override
        public Object getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(Object defaultValue) {
            this.defaultValue = defaultValue;
        }

        @Override
        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public void setPrimaryKey(boolean primaryKey) {
            this.primaryKey = primaryKey;
        }

        @Override
        public boolean isUnique() {
            return isUnique;
        }

        public void setUnique(boolean unique) {
            isUnique = unique;
        }
    }

    private static final class ForeignKeyImpl implements ForeignKey {
        private final String localTableName, localFieldName, foreignTableName, foreignFieldName;

        public ForeignKeyImpl(String localTableName, String localFieldName, String foreignTableName, String foreignFieldName) {
            this.localTableName = localTableName;
            this.localFieldName = localFieldName;
            this.foreignTableName = foreignTableName;
            this.foreignFieldName = foreignFieldName;
        }

        public String getLocalTableName() {
            return localTableName;
        }

        public String getLocalFieldName() {
            return localFieldName;
        }

        public String getForeignTableName() {
            return foreignTableName;
        }

        public String getForeignFieldName() {
            return foreignFieldName;
        }
    }

    private static final class IndexImpl implements Index {
        private final String indexName;
        private final String tableName;
        private final Collection<String> fieldNames;

        public IndexImpl(String indexName, String tableName, Collection<String> fieldNames) {
            this.indexName = indexName;
            this.tableName = tableName;
            this.fieldNames = fieldNames;
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        @Override
        public Collection<String> getFieldNames() {
            return fieldNames;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public String toString() {
            return "IndexImpl{" +
                    "indexName='" + indexName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", fieldNames=" + fieldNames +
                    '}';
        }
    }
}
