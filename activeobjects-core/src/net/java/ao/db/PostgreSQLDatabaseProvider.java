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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.schema.Case;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;

public class PostgreSQLDatabaseProvider extends DatabaseProvider
{
    private static final int MAX_SEQUENCE_LENGTH = 64;
    private static final String SQL_STATE_UNDEFINED_FUNCTION = "42883";
    private static final Pattern PATTERN_QUOTE_ID = Pattern.compile("(\\*|\\d*?)");

    public PostgreSQLDatabaseProvider(DisposableDataSource dataSource)
    {
        this(dataSource, "public");
    }

    public PostgreSQLDatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        super(dataSource, schema, TypeManager.postgres());
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

			case Types.BIT:
				try {
					return Byte.parseByte(value);
				} catch (Throwable t) {
					try {
						return Boolean.parseBoolean(value);
					} catch (Throwable t1) {
						return null;
					}
				}
		}

		return super.parseValue(type, value);
	}

	@Override
	public ResultSet getTables(Connection conn) throws SQLException {
		return conn.getMetaData().getTables(null, getSchema(), null, new String[] {"TABLE"});
	}

	@Override
	protected String renderAutoIncrement() {
		return "";
	}

    @Override
    protected String renderFieldType(DDLField field)
    {
        if (field.getJdbcType() == Types.NUMERIC) // numeric is used by Oracle
        {
            field.setType(typeManager.getType(Integer.class));
        }
        if (field.isAutoIncrement())
        {
            if (field.getJdbcType() == Types.BIGINT)
            {
                return "BIGSERIAL";
            }
            return "SERIAL";
        }
        return super.renderFieldType(field);
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
    protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
        return "CONSTRAINT " + uniqueNameConverter.getName(table.getName(), field.getName()) + " UNIQUE";
    }

    @Override
    public Object handleBlob(ResultSet res, Class<?> type, String field) throws SQLException
    {
        if (type.equals(InputStream.class))
        {
            return res.getBinaryStream(field);
        }
        else if (type.equals(byte[].class))
        {
            return res.getBytes(field);
        }
        else
        {
            return null;
        }
    }

    @Override
	protected Iterable<SQLAction> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
	{
        final UniqueNameConverter uniqueNameConverter = nameConverters.getUniqueNameConverter();

        final List<SQLAction> back = Lists.newArrayList();

        if (!field.isUnique() && oldField.isUnique())
        {
            // use oldField here (in case of a renamed column we need the old name)
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" DROP CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), oldField.getName()))));
        }

        if (field.isUnique() && !oldField.isUnique())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" ADD CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName())).append(" UNIQUE (").append(processID(field.getName())).append(")")));
        }

		if (!field.getName().equalsIgnoreCase(oldField.getName())) {
			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" RENAME COLUMN ");
			str.append(processID(oldField.getName())).append(" TO ").append(processID(field.getName()));
			back.add(SQLAction.of(str));
		}

		if (!field.getType().equals(oldField.getType())) {
			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" TYPE ");
            
            final boolean autoIncrement = field.isAutoIncrement();
            field.setAutoIncrement(false); // we don't want the auto increment property to be changed or even affect the change
            
			str.append(renderFieldType(field));
            back.add(SQLAction.of(str));

            field.setAutoIncrement(autoIncrement); // setting back to normal
		}

		if (field.getDefaultValue() == null && oldField.getDefaultValue() == null) {
			// dummy case
		} else if (field.getDefaultValue() == null && oldField.getDefaultValue() != null) {
			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" DROP DEFAULT");
            back.add(SQLAction.of(str));
		} else if (!field.getDefaultValue().equals(oldField.getDefaultValue())) {
			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" SET DEFAULT ").append(renderValue(field.getDefaultValue()));
            back.add(SQLAction.of(str));
		}

		if (field.isNotNull() != oldField.isNotNull()) {
			if (field.isNotNull()) {
				StringBuilder str = new StringBuilder();
				str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
				str.append(processID(field.getName())).append(" SET NOT NULL");
	            back.add(SQLAction.of(str));
			} else {
				StringBuilder str = new StringBuilder();
				str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
				str.append(processID(field.getName())).append(" DROP NOT NULL");
	            back.add(SQLAction.of(str));
			}
		}

		// if we don't have any ALTER TABLE DDL by this point then fall back to dropping and re-creating the column
		if (back.isEmpty()) {
			System.err.println("WARNING: Unable to modify column '" + table.getName() + "' in place. Going to drop and re-create column.");
			System.err.println("WARNING: Data contained in column '" + table.getName() + "." + oldField.getName() + "' will be lost");

			Iterables.addAll(back, renderAlterTableDropColumn(nameConverters, table, oldField));
			Iterables.addAll(back, renderAlterTableAddColumn(nameConverters, table, field));
		}

        return ImmutableList.<SQLAction>builder()
                .addAll(renderDropAccessoriesForField(nameConverters, table, oldField))
                .addAll(back)
                .addAll(renderAccessoriesForField(nameConverters, table, field))
                .build();
    }

	@Override
	protected SQLAction renderAlterTableDropKey(DDLForeignKey key)
	{
		StringBuilder back = new StringBuilder("ALTER TABLE ");

		back.append(withSchema(key.getDomesticTable())).append(" DROP CONSTRAINT ").append(processID(key.getFKName()));

		return SQLAction.of(back);
	}

    @Override
    protected SQLAction renderCreateIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        return SQLAction.of(new StringBuilder().append("CREATE INDEX ")
                .append(processID(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField()))))
                .append(" ON ").append(withSchema(index.getTable()))
                .append('(').append(processID(index.getField())).append(')'));
    }

    @Override
    protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        final String indexName = getExistingIndexName(indexNameConverter, index);
        final String tableName = index.getTable();
        if (hasIndex(tableName,indexName))
        {
            return SQLAction.of(new StringBuilder("DROP INDEX ")
                    .append(withSchema(indexName)));
        }
        else
        {
            return null;
        }
    }

    @Override
    public <T extends RawEntity<K>, K> K insertReturningKey(EntityManager manager, Connection conn,
            Class<T> entityType, Class<K> pkType,
            String pkField, boolean pkIdentity, String table, DBParam... params) throws SQLException
    {
        K back = null;
		for (DBParam param : params) {
			if (param.getField().trim().equalsIgnoreCase(pkField)) {
				back = (K) param.getValue();
				break;
			}
		}

		if (back == null) {
			final String sql = "SELECT NEXTVAL('" + withSchema(sequenceName(pkField, table)) + "')";

			final PreparedStatement stmt = preparedStatement(conn, sql);

			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				 back = typeManager.getType(pkType).getLogicalType().pullFromDatabase(null, res, pkType, 1);
			}
			res.close();
			stmt.close();

			List<DBParam> newParams = new ArrayList<DBParam>();
			newParams.addAll(Arrays.asList(params));

			newParams.add(new DBParam(pkField, back));
			params = newParams.toArray(new DBParam[newParams.size()]);
		}

		super.insertReturningKey(manager, conn, entityType, pkType, pkField, pkIdentity, table, params);

		return back;
	}

    private String sequenceName(String pkField, String table)
    {
        final String suffix = "_" + pkField + "_seq";
        final int tableLength = table.length();
        final int theoreticalLength = tableLength + suffix.length();
        if (theoreticalLength > MAX_SEQUENCE_LENGTH)
        {
            final int extraCharacters = theoreticalLength - MAX_SEQUENCE_LENGTH;
            return table.substring(0, tableLength - extraCharacters - 1) + suffix;
        }
        else
        {
            return table + suffix;
        }
    }

    @Override
    protected <T extends RawEntity<K>, K> K executeInsertReturningKey(EntityManager manager, Connection conn, 
                                                                      Class<T> entityType, Class<K> pkType,
                                                                      String pkField, String sql, DBParam... params) throws SQLException
    {
		final PreparedStatement stmt = preparedStatement(conn, sql);

		for (int i = 0; i < params.length; i++) {
			Object value = params[i].getValue();

			if (value instanceof RawEntity<?>) {
				value = Common.getPrimaryKeyValue((RawEntity<Object>) value);
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

		return null;
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}

	@Override
	protected boolean shouldQuoteID(String id) {
        return !PATTERN_QUOTE_ID.matcher(id).matches();
    }

    @Override
    protected boolean shouldQuoteTableName(String tableName) {
        return getReservedWords().contains(Case.UPPER.apply(tableName));
    }

    @Override
    public void handleUpdateError(String sql, SQLException e) throws SQLException
    {
        if (e.getSQLState().equals(SQL_STATE_UNDEFINED_FUNCTION) && e.getMessage().contains("does not exist"))
        {
            logger.debug("Ignoring SQL exception for <" + sql + ">", e);
            return;
        }
        super.handleUpdateError(sql, e);
    }

    private static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "ABS", "ABSOLUTE", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE",
            "ALIAS", "ALL", "ALLOCATE", "ALTER", "ANALYSE", "ANALYZE", "AND", "ANY", "ARE",
            "ARRAY", "AS", "ASC", "ASENSITIVE", "ASSERTION", "ASYMMETRIC", "AT", "ATOMIC",
            "AUTHORIZATION", "AVG", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BIT",
            "BIT_LENGTH", "BLOB", "BOOLEAN", "BOTH", "BREADTH", "BY", "CALL", "CALLED",
            "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CEIL",
            "CEILING", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK",
            "CLASS", "CLOB", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLLECT",
            "COLUMN", "COMMIT", "COMPLETION", "CONDITION", "CONNECT", "CONNECTION",
            "CONSTRAINT", "CONSTRAINTS", "CONSTRUCTOR", "CONTINUE", "CONVERT", "CORR",
            "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE",
            "CUME_DIST", "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP",
            "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE", "DATA",
            "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
            "DEFERRABLE", "DEFERRED", "DELETE", "DENSE_RANK", "DEPTH", "DEREF", "DESC",
            "DESCRIBE", "DESCRIPTOR", "DESTROY", "DESTRUCTOR", "DETERMINISTIC",
            "DIAGNOSTICS", "DICTIONARY", "DISCONNECT", "DISTINCT", "DO", "DOMAIN", "DOUBLE",
            "DROP", "DYNAMIC", "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "EQUALS",
            "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXP",
            "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER", "FIRST", "FLOAT", "FLOOR",
            "FOR", "FOREIGN", "FOUND", "FREE", "FREEZE", "FROM", "FULL", "FUNCTION",
            "FUSION", "GENERAL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GREATEST", "GROUP",
            "GROUPING", "HAVING", "HOLD", "HOST", "HOUR", "IDENTITY", "IGNORE", "ILIKE",
            "IMMEDIATE", "IN", "INDICATOR", "INITIALIZE", "INITIALLY", "INNER", "INOUT",
            "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERSECTION",
            "INTERVAL", "INTO", "IS", "ISNULL", "ISOLATION", "ITERATE", "JOIN", "KEY",
            "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEAST", "LEFT", "LESS",
            "LEVEL", "LIKE", "LIMIT", "LN", "LOCAL", "LOCALTIME", "LOCALTIMESTAMP", "LOCATOR",
            "LOWER", "MAP", "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE",
            "MOD", "MODIFIES", "MODIFY", "MODULE", "MONTH", "MULTISET", "NAMES", "NATIONAL",
            "NATURAL", "NCHAR", "NCLOB", "NEW", "NEXT", "NO", "NONE", "NORMALIZE", "NOT",
            "NOTNULL", "NULL", "NULLIF", "NUMERIC", "OBJECT", "OCTET_LENGTH", "OF", "OFF",
            "OFFSET", "OLD", "ON", "ONLY", "OPEN", "OPERATION", "OPTION", "OR", "ORDER",
            "ORDINALITY", "OUT", "OUTER", "OUTPUT", "OVER", "OVERLAPS", "OVERLAY", "PAD",
            "PARAMETER", "PARAMETERS", "PARTIAL", "PARTITION", "PATH", "PERCENTILE_CONT",
            "PERCENTILE_DISC", "PERCENT_RANK", "PLACING", "POSITION", "POSTFIX", "POWER",
            "PRECISION", "PREFIX", "PREORDER", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR",
            "PRIVILEGES", "PROCEDURE", "PUBLIC", "RANGE", "RANK", "READ", "READS", "REAL",
            "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY",
            "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY",
            "REGR_SYY", "RELATIVE", "RELEASE", "RESTRICT", "RESULT", "RETURN", "RETURNING",
            "RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROUTINE", "ROW",
            "ROWS", "ROW_NUMBER", "SAVEPOINT", "SCHEMA", "SCOPE", "SCROLL", "SEARCH", "SECOND",
            "SECTION", "SELECT", "SENSITIVE", "SEQUENCE", "SESSION", "SESSION_USER", "SET",
            "SETOF", "SETS", "SIMILAR", "SIZE", "SMALLINT", "SOME", "SPACE", "SPECIFIC",
            "SPECIFICTYPE", "SQL", "SQLCODE", "SQLERROR", "SQLEXCEPTION", "SQLSTATE",
            "SQLWARNING", "SQRT", "START", "STATE", "STATEMENT", "STATIC", "STDDEV_POP",
            "STDDEV_SAMP", "STRUCTURE", "SUBMULTISET", "SUBSTRING", "SUM", "SYMMETRIC",
            "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEMPORARY", "TERMINATE", "THAN",
            "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSACTION", "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE",
            "UESCAPE", "UNDER", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER",
            "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARIABLE", "VARYING",
            "VAR_POP", "VAR_SAMP", "VERBOSE", "VIEW", "WHEN", "WHENEVER", "WHERE",
            "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRITE", "YEAR", "ZONE");
}
