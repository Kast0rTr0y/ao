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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.EntityManager;
import net.java.ao.Query;
import net.java.ao.RawEntity;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;

import static net.java.ao.sql.SqlUtils.closeQuietly;
import static net.java.ao.types.LogicalTypes.blobType;
import static net.java.ao.types.LogicalTypes.booleanType;
import static net.java.ao.types.LogicalTypes.dateType;
import static net.java.ao.types.LogicalTypes.doubleType;
import static net.java.ao.types.LogicalTypes.enumType;
import static net.java.ao.types.LogicalTypes.floatType;
import static net.java.ao.types.LogicalTypes.integerType;
import static net.java.ao.types.LogicalTypes.longType;
import static net.java.ao.types.SchemaProperties.schemaType;

/**
 * @author Daniel Spiewak
 */
public class HSQLDatabaseProvider extends DatabaseProvider
{
    public HSQLDatabaseProvider(DisposableDataSource dataSource)
    {
        this(dataSource, "PUBLIC");
    }

    public HSQLDatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        super(dataSource, schema,
              new TypeManager.Builder()
                .addMapping(blobType(), schemaType("BINARY"))
                .addMapping(booleanType(), schemaType("BOOLEAN"))
                .addMapping(dateType(), schemaType("DATETIME"))
                .addMapping(doubleType(), schemaType("DOUBLE"))
                .addMapping(enumType(), schemaType("INTEGER"))
                .addMapping(floatType(), schemaType("FLOAT"))
                .addMapping(integerType(), schemaType("INTEGER"))
                .addMapping(longType(), schemaType("BIGINT"))
                .addStringTypes("VARCHAR", "LONGVARCHAR")
                .build());
    }

    @Override
	@SuppressWarnings("unused")
    public <T extends RawEntity<K>, K> K insertReturningKey(EntityManager manager, Connection conn,
                                                            Class<T> entityType, Class<K> pkType,
                                                            String pkField, boolean pkIdentity, String table, DBParam... params) throws SQLException
    {
		StringBuilder sql = new StringBuilder("INSERT INTO " + processID(table) + " (");

		for (DBParam param : params) {
			sql.append(processID(param.getField()));
			sql.append(',');
		}
		if (params.length > 0) {
			sql.setLength(sql.length() - 1);
		} else {
			sql.append(processID(pkField));
		}

		sql.append(") VALUES (");

		for (DBParam param : params) {
			sql.append("?,");
		}
		if (params.length > 0) {
			sql.setLength(sql.length() - 1);
		} else {
			sql.append("NULL");
		}

		sql.append(")");

		return executeInsertReturningKey(manager, conn, entityType, pkType, pkField, sql.toString(), params);
	}

	@Override
    protected synchronized <T extends RawEntity<K>, K> K executeInsertReturningKey(EntityManager manager, Connection conn, 
            Class<T> entityType, Class<K> pkType,
            String pkField, String sql, DBParam... params) throws SQLException
    {
	    K back = null;

		PreparedStatement stmt = preparedStatement(conn, sql);

		for (int i = 0; i < params.length; i++) {
			Object value = params[i].getValue();

			if (value instanceof RawEntity<?>) {
				value = Common.getPrimaryKeyValue((RawEntity<Object>) value);
			}

			if (params[i].getField().equalsIgnoreCase(pkField)) {
				back = (K) value;
			}

			if (value == null) {
				putNull(stmt, i + 1);
			} else {
				TypeInfo<Object> type = (TypeInfo<Object>) typeManager.getType(value.getClass());
				type.getLogicalType().putToDatabase(manager, stmt, i + 1, value, type.getJdbcWriteType());
			}
		}

		stmt.executeUpdate();
		stmt.close();

		if (back == null) {
			stmt = conn.prepareStatement("CALL IDENTITY()");		// WARNING	potential breakage here if dealing with INSERTs outside ORM control

			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				 back = typeManager.getType(pkType).getLogicalType().pullFromDatabase(null, res, pkType, 1);
			}
			res.close();
			stmt.close();
		}

		return back;
	}

	@Override
	public Object parseValue(int type, String value) {
		if (value == null || value.equals("") || value.equals("NULL")) {
			return null;
		}

		switch (type) {
			case Types.TIMESTAMP:
			case Types.DATE:
			case Types.TIME:
            case Types.VARCHAR:
				Matcher matcher = Pattern.compile("'(.*)'.*").matcher(value);
				if (matcher.find()) {
					value = matcher.group(1);
				}
			break;
		}

		return super.parseValue(type, value);
	}

	@Override
	public ResultSet getTables(Connection conn) throws SQLException {
		return conn.getMetaData().getTables(null, getSchema(), null, new String[] {"TABLE"});
	}

    @Override
    public void dispose()
    {
        Connection conn = null;
        Statement stmt = null;
        try
        {
            conn = getConnection();
            stmt = conn.createStatement();
            stmt.executeUpdate("SHUTDOWN");
        }
        catch (SQLException e)
        {
            // ignored
        }
        finally
        {
            closeQuietly(stmt);
            closeQuietly(conn);
        }
        super.dispose();
    }

    @Override
	protected String renderQuerySelect(Query query, TableNameConverter converter, boolean count) {
		StringBuilder sql = new StringBuilder();
		String tableName = query.getTable();

		if (tableName == null) {
			tableName = converter.getName(query.getTableType());
		}

		switch (query.getType()) {
			case SELECT:
				sql.append("SELECT ");

				if (query.isDistinct()) {
					sql.append("DISTINCT ");
				}

				int limit = query.getLimit();
				if (limit >= 0) {
					int offset = Math.max(query.getOffset(), 0);

					sql.append("LIMIT ").append(offset).append(' ');
					sql.append(limit).append(' ');
				}

				if (count) {
					sql.append("COUNT(*)");
				} else {
					StringBuilder fields = new StringBuilder();
					for (String field : query.getFields()) {
						fields.append(processID(field)).append(',');
					}
					if (query.getFields().length > 0) {
						fields.setLength(fields.length() - 1);
					}
					
					sql.append(fields);
				}
				sql.append(" FROM ");

				sql.append(withSchema(tableName));
			break;
		}

		return sql.toString();
	}

	@Override
	protected String renderQueryLimit(Query query) {
		return "";
	}

    @Override
	protected String renderAutoIncrement() {
		return "GENERATED BY DEFAULT AS IDENTITY (START WITH 1)";
	}

	@Override
	protected String getDateFormat() {
		return "yyyy-MM-dd HH:mm:ss.SSS";
	}

	@Override
	protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field) {
		return "";
	}

	@Override
	protected String renderConstraintsForTable(UniqueNameConverter uniqueNameConverter, DDLTable table) {
		StringBuilder back = new StringBuilder(super.renderConstraintsForTable(uniqueNameConverter, table));

		for (DDLField field : table.getFields()) {
			if (field.isUnique()) {
				back.append(" CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName())).append(" UNIQUE(").append(processID(field.getName())).append("),\n");
			}
		}

		return back.toString();
	}

	@Override
	protected String renderValue(Object value) {
		if (value instanceof Boolean) {
			if (value.equals(true)) {
				return "TRUE";
			}

			return "FALSE";
		}

		return super.renderValue(value);
	}

    @Override
    protected List<String> renderAlterTableAddColumn(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        final List<String> back = super.renderAlterTableAddColumn(nameConverters, table, field);

        if (field.isUnique())
        {
            back.add(renderAddUniqueConstraint(nameConverters.getUniqueNameConverter(), table, field));
        }
        return back;
    }

    @Override
    protected List<String> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
       final List<String> sql = super.renderAlterTableChangeColumn(nameConverters, table, oldField, field);

        if (!field.isPrimaryKey())
        {
            final UniqueNameConverter uniqueNameConverter = nameConverters.getUniqueNameConverter();
            if (!oldField.isUnique() && field.isUnique())
            {
                sql.add(renderAddUniqueConstraint(uniqueNameConverter, table, field));
            }
            if (oldField.isUnique() && !field.isUnique())
            {
                sql.add(new StringBuilder().append("ALTER TABLE ")
                        .append(withSchema(table.getName()))
                        .append(" DROP CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName()))
                        .toString());
            }
        }
        return sql;
    }

    private String renderAddUniqueConstraint(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
        return new StringBuilder()
                .append("ALTER TABLE ").append(withSchema(table.getName()))
                .append(" ADD CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName()))
                .append(" UNIQUE (").append(processID(field.getName())).append(")")
                .toString();
    }

	@Override
	protected String renderAlterTableChangeColumnStatement(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field, RenderFieldOptions options) {
		StringBuilder current = new StringBuilder();

		current.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
		current.append(renderField(nameConverters, table, field, options));

		return current.toString();
	}

	@Override
	protected String renderAlterTableDropKey(DDLForeignKey key) {
		StringBuilder back = new StringBuilder("ALTER TABLE ");

		back.append(withSchema(key.getDomesticTable())).append(" DROP CONSTRAINT ").append(processID(key.getFKName()));

		return back.toString();
	}

	@Override
	protected String renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index) {
		StringBuilder back = new StringBuilder("DROP INDEX ");

		back.append(withSchema(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField()))));

		return back.toString();
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}

	@Override
	public boolean isCaseSensetive() {
		return false;
	}

    private static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY",
            "AS", "ASENSITIVE", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "BEGIN",
            "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BY", "CALL", "CALLED", "CASCADED",
            "CASE", "CAST", "CHAR", "CHARACTER", "CHECK", "CLOB", "CLOSE", "COLLATE",
            "COLUMN", "COMMIT", "CONDIITON", "CONNECT", "CONSTRAINT", "CONTINUE",
            "CORRESPONDING", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE",
            "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER", "CURSOR", "CYCLE", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL",
            "DECLARE", "DEFAULT", "DELETE", "DEREF", "DESCRIBE", "DETERMINISTIC",
            "DISCONNECT", "DISTINCT", "DO", "DOUBLE", "DAYOFWEEK", "DROP", "DYNAMIC", "EACH",
            "ELEMENT", "ELSE", "ELSEIF", "END", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE",
            "EXISTS", "EXIT", "EXTERNAL", "FALSE", "FETCH", "FILTER", "FLOAT", "FOR",
            "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION", "GET", "GLOBAL", "GRANT",
            "GROUP", "GROUPING", "HANDLER", "HAVING", "HEADER", "HOLD", "HOUR", "IDENTITY",
            "IF", "IMMEDIATE", "IN", "INDICATOR", "INNER", "INOUT", "INPUT", "INSENSITIVE",
            "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ITERATE",
            "JOIN", "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEAVE", "LEFT", "LIKE",
            "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOOP", "MATCH", "MEMBER", "METHOD",
            "MINUTE", "MODIFIES", "MODULE", "MONTH", "MULTISET", "NATIONAL", "NAUTRAL",
            "NCHAR", "NCLOB", "NEW", "NEXT", "NO", "NONE", "NOT", "NULL", "NUMERIC", "OF",
            "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OUTPUT", "OVER",
            "OVERLAPS", "PARAMETER", "PARTITION", "PRECISION", "PREPARE", "PRIMARY",
            "PROCEDURE", "RANGE", "READS", "REAL", "RECURSIVE", "REF", "REFERENCES",
            "REFERENCING", "RELEASE", "REPEAT", "RESIGNAL", "RESULT", "RETURN", "RETURNS",
            "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "SAVEPOINT", "SCOPE",
            "SCROLL", "SECOND", "SEARCH", "SELECT", "SENSITIVE", "SESSION_USER", "SET",
            "SIGNAL", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE", "SQL",
            "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "START", "STATIC", "SUBMULTISET",
            "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "THEN", "TIME",
            "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSLATION",
            "TREAT", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNKNOWN", "UNNEST",
            "UNTIL", "UPDATE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
            "WHEN", "WHENEVER", "WHERE", "WHILE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "YEAR",
            "MIN", "MAX", "POSITION");
}
