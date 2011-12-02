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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.ao.types.VarcharType;

import static net.java.ao.types.StringTypeProperties.stringType;

import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.BlobType;
import net.java.ao.types.ClobType;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.DoubleType;
import net.java.ao.types.TypeManager;

import static net.java.ao.types.NumericTypeProperties.numericType;

public final class PostgreSQLDatabaseProvider extends DatabaseProvider {

    private static final int MAX_SEQUENCE_LENGTH = 64;

	private static final Set<String> RESERVED_WORDS = new HashSet<String>() {
		{
			addAll(Arrays.asList("ABS", "ABSOLUTE", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE",
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
					"WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRITE", "YEAR", "ZONE"));
		}
	};

    private static final String SQL_STATE_UNDEFINED_FUNCTION = "42883";

    public PostgreSQLDatabaseProvider(DisposableDataSource dataSource)
    {
        this(dataSource, "public");
    }

    public PostgreSQLDatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        super(dataSource, schema,
              new TypeManager.Builder()
                .addMapping(new VarcharType(stringType("VARCHAR", "TEXT")))
                .addMapping(new ClobType("TEXT"))
                .addMapping(new BlobType("BYTEA"))
                .addMapping(new DoubleType(numericType("DOUBLE PRECISION").ignorePrecision(true)))
                .build());
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
        if (field.getType().getType() == Types.NUMERIC) // numeric is used by Oracle
        {
            field.setType(typeManager.getType(Types.INTEGER));
        }
        if (field.isAutoIncrement())
        {
            if (field.getType().getType() == Types.BIGINT)
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
	protected String renderFunction(DatabaseFunction func) {
		switch (func) {
			case CURRENT_DATE:
				return "now()";

			case CURRENT_TIMESTAMP:
				return "now()";
		}

		return super.renderFunction(func);
	}

	@Override
	protected String renderFunctionForField(TriggerNameConverter triggerNameConverter, DDLTable table, DDLField field) {
		Object onUpdate = field.getOnUpdate();
		if (onUpdate != null) {
			StringBuilder back = new StringBuilder();

            final String triggerName = triggerNameConverter.onUpdateName(table.getName(), field.getName());

            back.append("CREATE FUNCTION ").append(isSchemaNotEmpty() ? getSchema() + "." + triggerName : triggerName).append("()");
            back.append(" RETURNS trigger AS $").append(triggerName).append("$\nBEGIN\n");
			back.append("    NEW.").append(processID(field.getName())).append(" := ").append(renderValue(onUpdate));
            back.append(";\n    RETURN NEW;\nEND;\n$").append(triggerName).append("$ LANGUAGE plpgsql");

			return back.toString();
		}

		return super.renderFunctionForField(triggerNameConverter, table, field);
	}

    @Override
	protected String renderTriggerForField(TriggerNameConverter triggerNameConverter, SequenceNameConverter sequenceNameConverter, DDLTable table, DDLField field) {
		Object onUpdate = field.getOnUpdate();
		if (onUpdate != null) {
			StringBuilder back = new StringBuilder();

            final String triggerName = triggerNameConverter.onUpdateName(table.getName(), field.getName());

            back.append("CREATE TRIGGER ").append(triggerName).append('\n');
            back.append(" BEFORE UPDATE OR INSERT ON ").append(withSchema(table.getName())).append('\n');
			back.append("    FOR EACH ROW EXECUTE PROCEDURE ");
            back.append(isSchemaNotEmpty() ? getSchema() + "." + triggerName : triggerName).append("()");

            return back.toString();
		}

		return super.renderTriggerForField(triggerNameConverter, sequenceNameConverter, table, field);
	}

    @Override
    protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
        return "CONSTRAINT " + uniqueNameConverter.getName(table.getName(), field.getName()) + " UNIQUE";
    }

	@Override
	protected String renderOnUpdate(DDLField field) {
		return "";
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
	protected List<String> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field) {
        final TriggerNameConverter triggerNameConverter = nameConverters.getTriggerNameConverter();
        final SequenceNameConverter sequenceNameConverter = nameConverters.getSequenceNameConverter();
        final UniqueNameConverter uniqueNameConverter = nameConverters.getUniqueNameConverter();

        final List<String> back = new ArrayList<String>();

        String trigger = getTriggerNameForField(triggerNameConverter, table, oldField);
		if (trigger != null) {
			StringBuilder str = new StringBuilder();
			str.append("DROP TRIGGER ").append(processID(trigger));
			back.add(str.toString());
		}

		String function = getFunctionNameForField(triggerNameConverter, table, oldField);
		if (function != null) {
			StringBuilder str = new StringBuilder();
			str.append("DROP FUNCTION IF EXISTS ").append(withSchema(function));
			back.add(str.toString());
		}

        if (!field.isUnique() && oldField.isUnique())
        {
            back.add(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" DROP CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName())).toString());
        }

        if (field.isUnique() && !oldField.isUnique())
        {
            back.add(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" ADD CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName())).append(" UNIQUE (").append(processID(field.getName())).append(")").toString());
        }

		boolean foundChange = false;
		if (!field.getName().equalsIgnoreCase(oldField.getName())) {
			foundChange = true;

			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" RENAME COLUMN ");
			str.append(processID(oldField.getName())).append(" TO ").append(processID(field.getName()));
			back.add(str.toString());
		}

		if (!field.getType().equals(oldField.getType())) {
			foundChange = true;

			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" TYPE ");
			str.append(renderFieldType(field));
			back.add(str.toString());
		}

		if (field.getDefaultValue() == null && oldField.getDefaultValue() == null) {
			// dummy case
		} else if (field.getDefaultValue() == null && oldField.getDefaultValue() != null) {
			foundChange = true;

			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" DROP DEFAULT");
			back.add(str.toString());
		} else if (!field.getDefaultValue().equals(oldField.getDefaultValue())) {
			foundChange = true;

			StringBuilder str = new StringBuilder();
			str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
			str.append(processID(field.getName())).append(" SET DEFAULT ").append(renderValue(field.getDefaultValue()));
			back.add(str.toString());
		}

		if (field.isNotNull() != oldField.isNotNull()) {
			foundChange = true;

			if (field.isNotNull()) {
				StringBuilder str = new StringBuilder();
				str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
				str.append(processID(field.getName())).append(" SET NOT NULL");
				back.add(str.toString());
			} else {
				StringBuilder str = new StringBuilder();
				str.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
				str.append(processID(field.getName())).append(" DROP NOT NULL");
				back.add(str.toString());
			}
		}

		if (!foundChange) {
			System.err.println("WARNING: PostgreSQL doesn't fully support CHANGE TABLE statements");
			System.err.println("WARNING: Data contained in column '" + table.getName() + "." + oldField.getName() + "' will be lost");

			back.addAll(Arrays.asList(renderAlterTableDropColumn(triggerNameConverter, table, oldField)));
			back.addAll(renderAlterTableAddColumn(nameConverters, table, field));
		}

		String toRender = renderFunctionForField(triggerNameConverter, table, field);
		if (toRender != null) {
			back.add(toRender);
		}

		toRender = renderTriggerForField(triggerNameConverter, sequenceNameConverter, table, field);
		if (toRender != null) {
			back.add(toRender);
		}

        return back;
    }

	@Override
	protected String renderAlterTableDropKey(DDLForeignKey key) {
		StringBuilder back = new StringBuilder("ALTER TABLE ");

		back.append(withSchema(key.getDomesticTable())).append(" DROP CONSTRAINT ").append(processID(key.getFKName()));

		return back.toString();
	}

    @Override
    protected String renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        return new StringBuilder("DROP INDEX ")
                .append(withSchema(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField()))))
                .toString();
    }

    protected String[] renderDropFunctions(TriggerNameConverter triggerNameConverter, DDLTable table)
    {
        final List<String> dropFunctions = new ArrayList<String>();
        for (DDLField field : table.getFields())
        {
            field.setOnUpdate(new Object()); // kind of a hack
            dropFunctions.add(renderDropFunction(getFunctionNameForField(triggerNameConverter, table, field)));
        }
        return dropFunctions.toArray(new String[dropFunctions.size()]);
    }

    private String renderDropFunction(String functionName)
    {
        return "DROP FUNCTION IF EXISTS " + (isSchemaNotEmpty() ? getSchema() + "." + functionName : functionName)+ " CASCADE";
    }


    @Override
    protected String getTriggerNameForField(TriggerNameConverter triggerNameConverter, DDLTable table, DDLField field)
    {
        if (field.getOnUpdate() != null)
        {
            return triggerNameConverter.onUpdateName(table.getName(), field.getName());
        }

        return super.getTriggerNameForField(triggerNameConverter, table, field);
    }

    @Override
    public synchronized <T extends RawEntity<K>, K> K insertReturningKey(EntityManager manager, Connection conn,
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
			final String sql = "SELECT NEXTVAL('" + processID(sequenceName(pkField, table)) + "')";

			final PreparedStatement stmt = preparedStatement(conn, sql);

			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				 back = typeManager.getType(pkType).pullFromDatabase(null, res, pkType, 1);
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
				DatabaseType<Object> type = (DatabaseType<Object>) typeManager.getType(value.getClass());
				type.putToDatabase(manager, stmt, i + 1, value);
			}
		}

		stmt.executeUpdate();
		stmt.close();

		return null;
	}

	@Override
	public void putNull(PreparedStatement stmt, int index) throws SQLException {
		stmt.setNull(index, stmt.getParameterMetaData().getParameterType(index));
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}

	@Override
	protected boolean shouldQuoteID(String id) {
        return !"*".equals(id);
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
}