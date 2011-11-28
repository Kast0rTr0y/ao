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

import net.java.ao.DisposableDataSource;
import net.java.ao.DBParam;
import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.Query;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.DatabaseType;

/**
 * @author Daniel Spiewak
 */
public class SQLServerDatabaseProvider extends DatabaseProvider {
	private static final Pattern VALUE_PATTERN = Pattern.compile("^\\((.*?)\\)$");
	private static final Set<String> RESERVED_WORDS = new HashSet<String>() {
		{
			addAll(Arrays.asList("ADD", "EXCEPT", "PERCENT", "ALL", "EXEC", "PLAN", "ALTER", "EXECUTE",
					"PRECISION", "AND", "EXISTS", "PRIMARY", "ANY", "EXIT", "PRINT", "AS", "FETCH", "PROC",
					"ASC", "FILE", "PROCEDURE", "AUTHORIZATION", "FILLFACTOR", "PUBLIC", "BACKUP", "FOR",
					"RAISERROR", "BEGIN", "FOREIGN", "READ", "BETWEEN", "FREETEXT", "READTEXT", "BREAK",
					"FREETEXTTABLE", "RECONFIGURE", "BROWSE", "FROM", "REFERENCES", "BULK", "FULL",
					"REPLICATION", "BY", "FUNCTION", "RESTORE", "CASCADE", "GOTO", "RESTRICT", "CASE",
					"GRANT", "RETURN", "CHECK", "GROUP", "REVOKE", "CHECKPOINT", "HAVING", "RIGHT", "CLOSE",
					"HOLDLOCK", "ROLLBACK", "CLUSTERED", "IDENTITY", "ROWCOUNT", "COALESCE", "IDENTITY_INSERT",
					"ROWGUIDCOL", "COLLATE", "IDENTITYCOL", "RULE", "COLUMN", "IF", "SAVE", "COMMIT", "IN",
					"SCHEMA", "COMPUTE", "INDEX", "SELECT", "CONSTRAINT", "INNER", "SESSION_USER", "CONTAINS",
					"INSERT", "SET", "CONTAINSTABLE", "INTERSECT", "SETUSER", "CONTINUE", "INTO", "SHUTDOWN",
					"CONVERT", "IS", "SOME", "CREATE", "JOIN", "STATISTICS", "CROSS", "KEY", "SYSTEM_USER",
					"CURRENT", "KILL", "TABLE", "CURRENT_DATE", "LEFT", "TEXTSIZE", "CURRENT_TIME", "LIKE",
					"THEN", "CURRENT_TIMESTAMP", "LINENO", "TO", "CURRENT_USER", "LOAD", "TOP", "CURSOR",
					"NATIONAL", "TRAN", "DATABASE", "NOCHECK", "TRANSACTION", "DBCC", "NONCLUSTERED",
					"TRIGGER", "DEALLOCATE", "NOT", "TRUNCATE", "DECLARE", "NULL", "TSEQUAL", "DEFAULT",
					"NULLIF", "UNION", "DELETE", "OF", "UNIQUE", "DENY", "OFF", "UPDATE", "DESC", "OFFSETS",
					"UPDATETEXT", "DISK", "ON", "USE", "DISTINCT", "OPEN", "USER", "DISTRIBUTED",
					"OPENDATASOURCE", "VALUES", "DOUBLE", "OPENQUERY", "VARYING", "DROP", "OPENROWSET",
					"VIEW", "DUMMY", "OPENXML", "WAITFOR", "DUMP", "OPTION", "WHEN", "ELSE", "OR", "WHERE",
					"END", "ORDER", "WHILE", "ERRLVL", "OUTER", "WITH", "ESCAPE", "OVER", "WRITETEXT",
					"ABSOLUTE", "EXEC", "OVERLAPS", "ACTION", "EXECUTE", "PAD", "ADA", "EXISTS", "PARTIAL",
					"ADD", "EXTERNAL", "PASCAL", "ALL", "EXTRACT", "POSITION", "ALLOCATE", "FALSE",
					"PRECISION", "ALTER", "FETCH", "PREPARE", "AND", "FIRST", "PRESERVE", "ANY", "FLOAT",
					"PRIMARY", "ARE", "FOR", "PRIOR", "AS", "FOREIGN", "PRIVILEGES", "ASC", "FORTRAN",
					"PROCEDURE", "ASSERTION", "FOUND", "PUBLIC", "AT", "FROM", "READ", "AUTHORIZATION",
					"FULL", "REAL", "AVG", "GET", "REFERENCES", "BEGIN", "GLOBAL", "RELATIVE", "BETWEEN",
					"GO", "RESTRICT", "BIT", "GOTO", "REVOKE", "BIT_LENGTH", "GRANT", "RIGHT", "BOTH",
					"GROUP", "ROLLBACK", "BY", "HAVING", "ROWS", "CASCADE", "HOUR", "SCHEMA", "CASCADED",
					"IDENTITY", "SCROLL", "CASE", "IMMEDIATE", "SECOND", "CAST", "IN", "SECTION", "CATALOG",
					"INCLUDE", "SELECT", "CHAR", "INDEX", "SESSION", "CHAR_LENGTH", "INDICATOR",
					"SESSION_USER", "CHARACTER", "INITIALLY", "SET", "CHARACTER_LENGTH", "INNER", "SIZE",
					"CHECK", "INPUT", "SMALLINT", "CLOSE", "INSENSITIVE", "SOME", "COALESCE", "INSERT",
					"SPACE", "COLLATE", "INT", "SQL", "COLLATION", "INTEGER", "SQLCA", "COLUMN", "INTERSECT",
					"SQLCODE", "COMMIT", "INTERVAL", "SQLERROR", "CONNECT", "INTO", "SQLSTATE", "CONNECTION",
					"IS", "SQLWARNING", "CONSTRAINT", "ISOLATION", "SUBSTRING", "CONSTRAINTS", "JOIN", "SUM",
					"CONTINUE", "KEY", "SYSTEM_USER", "CONVERT", "LANGUAGE", "TABLE", "CORRESPONDING", "LAST",
					"TEMPORARY", "COUNT", "LEADING", "THEN", "CREATE", "LEFT", "TIME", "CROSS", "LEVEL",
					"TIMESTAMP", "CURRENT", "LIKE", "TIMEZONE_HOUR", "CURRENT_DATE", "LOCAL", "TIMEZONE_MINUTE",
					"CURRENT_TIME", "LOWER", "TO", "CURRENT_TIMESTAMP", "MATCH", "TRAILING", "CURRENT_USER",
					"MAX", "TRANSACTION", "CURSOR", "MIN", "TRANSLATE", "DATE", "MINUTE", "TRANSLATION", "DAY",
					"MODULE", "TRIM", "DEALLOCATE", "MONTH", "TRUE", "DEC", "NAMES", "UNION", "DECIMAL",
					"NATIONAL", "UNIQUE", "DECLARE", "NATURAL", "UNKNOWN", "DEFAULT", "NCHAR", "UPDATE",
					"DEFERRABLE", "NEXT", "UPPER", "DEFERRED", "NO", "USAGE", "DELETE", "NONE", "USER", "DESC",
					"NOT", "USING", "DESCRIBE", "NULL", "VALUE", "DESCRIPTOR", "NULLIF", "VALUES", "DIAGNOSTICS",
					"NUMERIC", "VARCHAR", "DISCONNECT", "OCTET_LENGTH", "VARYING", "DISTINCT", "OF", "VIEW",
					"DOMAIN", "ON", "WHEN", "DOUBLE", "ONLY", "WHENEVER", "DROP", "OPEN", "WHERE", "ELSE",
					"OPTION", "WITH", "END", "OR", "WORK", "END-EXEC", "ORDER", "WRITE", "ESCAPE", "OUTER",
					"YEAR", "EXCEPT", "OUTPUT", "ZONE", "EXCEPTION", "ABSOLUTE", "FOUND", "PRESERVE", "ACTION",
					"FREE", "PRIOR", "ADMIN", "GENERAL", "PRIVILEGES", "AFTER", "GET", "READS", "AGGREGATE",
					"GLOBAL", "REAL", "ALIAS", "GO", "RECURSIVE", "ALLOCATE", "GROUPING", "REF", "ARE", "HOST",
					"REFERENCING", "ARRAY", "HOUR", "RELATIVE", "ASSERTION", "IGNORE", "RESULT", "AT",
					"IMMEDIATE", "RETURNS", "BEFORE", "INDICATOR", "ROLE", "BINARY", "INITIALIZE", "ROLLUP",
					"BIT", "INITIALLY", "ROUTINE", "BLOB", "INOUT", "ROW", "BOOLEAN", "INPUT", "ROWS", "BOTH",
					"INT", "SAVEPOINT", "BREADTH", "INTEGER", "SCROLL", "CALL", "INTERVAL", "SCOPE", "CASCADED",
					"ISOLATION", "SEARCH", "CAST", "ITERATE", "SECOND", "CATALOG", "LANGUAGE", "SECTION", "CHAR",
					"LARGE", "SEQUENCE", "CHARACTER", "LAST", "SESSION", "CLASS", "LATERAL", "SETS", "CLOB",
					"LEADING", "SIZE", "COLLATION", "LESS", "SMALLINT", "COMPLETION", "LEVEL", "SPACE", "CONNECT",
					"LIMIT", "SPECIFIC", "CONNECTION", "LOCAL", "SPECIFICTYPE", "CONSTRAINTS", "LOCALTIME", "SQL",
					"CONSTRUCTOR", "LOCALTIMESTAMP", "SQLEXCEPTION", "CORRESPONDING", "LOCATOR", "SQLSTATE",
					"CUBE", "MAP", "SQLWARNING", "CURRENT_PATH", "MATCH", "START", "CURRENT_ROLE", "MINUTE",
					"STATE", "CYCLE", "MODIFIES", "STATEMENT", "DATA", "MODIFY", "STATIC", "DATE", "MODULE",
					"STRUCTURE", "DAY", "MONTH", "TEMPORARY", "DEC", "NAMES", "TERMINATE", "DECIMAL", "NATURAL",
					"THAN", "DEFERRABLE", "NCHAR", "TIME", "DEFERRED", "NCLOB", "TIMESTAMP", "DEPTH", "NEW",
					"TIMEZONE_HOUR", "DEREF", "NEXT", "TIMEZONE_MINUTE", "DESCRIBE", "NO", "TRAILING", "DESCRIPTOR",
					"NONE", "TRANSLATION", "DESTROY", "NUMERIC", "TREAT", "DESTRUCTOR", "OBJECT", "TRUE",
					"DETERMINISTIC", "OLD", "UNDER", "DICTIONARY", "ONLY", "UNKNOWN", "DIAGNOSTICS", "OPERATION",
					"UNNEST", "DISCONNECT", "ORDINALITY", "USAGE", "DOMAIN", "OUT", "USING", "DYNAMIC", "OUTPUT",
					"VALUE", "EACH", "PAD", "VARCHAR", "END-EXEC", "PARAMETER", "VARIABLE", "EQUALS", "PARAMETERS",
					"WHENEVER", "EVERY", "PARTIAL", "WITHOUT", "EXCEPTION", "PATH", "WORK", "EXTERNAL", "POSTFIX",
					"WRITE", "FALSE", "PREFIX", "YEAR", "FIRST", "PREORDER", "ZONE", "FLOAT", "PREPARE"));
		}
	};

    public SQLServerDatabaseProvider(DisposableDataSource dataSource)
    {
        this(dataSource, "dbo");
    }

    public SQLServerDatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        super(dataSource, schema);
    }

	@Override
	public void setQueryResultSetProperties(ResultSet res, Query query) throws SQLException {
		if (query.getOffset() >= 0) {
			res.absolute(query.getOffset());
		}
	}

	@Override
	public ResultSet getTables(Connection conn) throws SQLException {
		return conn.getMetaData().getTables(null, getSchema(), null, new String[] {"TABLE"});
	}

	@Override
	public Object parseValue(int type, String value) {
        if (value == null || value.equals("") || value.equals("NULL"))
        {
            return null;
        }

        Matcher valueMatcher = VALUE_PATTERN.matcher(value);
        while (valueMatcher.matches())
        {
            value = valueMatcher.group(1);
            valueMatcher = VALUE_PATTERN.matcher(value);
        }

        switch (type)
        {
            case Types.TIMESTAMP:
            case Types.DATE:
            case Types.TIME:
            case Types.VARCHAR:
                Matcher matcher = Pattern.compile("'(.*)'.*").matcher(value);
                if (matcher.find())
                {
                    value = matcher.group(1);
                }
                break;
            case Types.BIT:
                try
                {
                    return Byte.parseByte(value);
                }
                catch (Throwable t)
                {
                    try
                    {
                        return Boolean.parseBoolean(value);
                    }
                    catch (Throwable t1)
                    {
                        return null;
                    }
                }
        }

        return super.parseValue(type, value);
    }

    @Override
    protected List<String> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
        final List<String> sql = super.renderAlterTableChangeColumn(nameConverters, table, oldField, field);

        if ((field.getDefaultValue() != null && !field.getDefaultValue().equals(oldField.getDefaultValue())) || (field.getDefaultValue() == null && oldField.getDefaultValue() != null))
        {
            sql.add(new StringBuilder()
                    .append("IF EXISTS (SELECT 1 FROM SYS.OBJECTS WHERE NAME = ").append(renderValue(defaultConstraintName(table, field))).append(") ")
                    .append("ALTER TABLE ").append(withSchema(table.getName()))
                    .append(" DROP CONSTRAINT ").append(defaultConstraintName(table, field))
                    .toString());

            if (field.getDefaultValue() != null)
            {
                sql.add(new StringBuilder()
                        .append("ALTER TABLE ").append(withSchema(table.getName()))
                        .append(" ADD CONSTRAINT ").append(defaultConstraintName(table, field))
                        .append(" DEFAULT ").append(renderValue(field.getDefaultValue()))
                        .append(" FOR ").append(processID(field.getName()))
                        .toString());
            }
        }

        if (!oldField.isUnique() && field.isUnique())
        {
            sql.add(new StringBuilder()
                    .append("ALTER TABLE ").append(withSchema(table.getName()))
                    .append(" ADD CONSTRAINT ").append(uniqueConstraintName(field))
                    .append(" UNIQUE(").append(processID(field.getName())).append(")")
                    .toString());
        }

        if (oldField.isUnique() && !field.isUnique())
        {
            sql.add(new StringBuilder()
                    .append("ALTER TABLE ").append(withSchema(table.getName()))
                    .append(" DROP CONSTRAINT ").append(uniqueConstraintName(oldField))
                    .toString());
        }

        return sql;
    }

    private String defaultConstraintName(DDLTable table, DDLField field)
    {
        return "df_" + table.getName() + '_' + field.getName();
    }

    private String uniqueConstraintName(DDLField field)
    {
        return "U_" + field.getName();
    }

    @Override
    protected RenderFieldOptions renderFieldOptionsInAlterColumn()
    {
        return new RenderFieldOptions(false, false);
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
					if (query.getOffset() > 0) {
						limit += query.getOffset();
					}

					sql.append("TOP ").append(limit).append(' ');
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
		return "IDENTITY(1,1)";
	}

	@Override
	protected String renderOnUpdate(DDLField field) {
		return "";
	}

	@Override
	protected String convertTypeToString(DatabaseType<?> type) {
		switch (type.getType()) {
			case Types.BOOLEAN:
				return "BIT";

			case Types.DOUBLE:
				return "DECIMAL";

			case Types.TIMESTAMP:
				return "DATETIME";

			case Types.DATE:
				return "SMALLDATETIME";

			case Types.CLOB:
			case Types.LONGVARCHAR:
				return "NTEXT";

			case Types.BLOB:
				return "IMAGE";
		}

		return super.convertTypeToString(type);
	}

	@Override
	protected boolean considerPrecision(DDLField field) {
		switch (field.getType().getType()) {
			case Types.INTEGER:
			case Types.BOOLEAN:
				return false;
		}

		return super.considerPrecision(field);
	}

    @Override
    protected String renderFieldDefault(DDLTable table, DDLField field)
    {
        return new StringBuilder().append(" CONSTRAINT ").append(defaultConstraintName(table, field)).append(" DEFAULT ").append(renderValue(field.getDefaultValue())).toString();
    }

    @Override
    protected String renderFieldPrecision(DDLField field)
    {
        switch (field.getType().getType())
        {
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                if (field.getPrecision() <= 0)
                {
                    field.setPrecision(32);
                }
                if (field.getScale() <= 0)
                {
                    field.setScale(16);
                }
        }
        return super.renderFieldPrecision(field);
    }

    @Override
	protected String renderFunction(DatabaseFunction func) {
		switch (func) {
			case CURRENT_DATE:
				return "GetDate()";

			case CURRENT_TIMESTAMP:
				return "GetDate()";
		}

		return super.renderFunction(func);
	}

	@Override
	protected String renderTriggerForField(TriggerNameConverter triggerNameConverter, SequenceNameConverter sequenceNameConverter, DDLTable table, DDLField field) {
		Object onUpdate = field.getOnUpdate();
		if (onUpdate != null) {
			StringBuilder back = new StringBuilder();

			DDLField pkField = null;
			for (DDLField f : table.getFields()) {
				if (f.isPrimaryKey()) {
					pkField = f;
					break;
				}
			}

			if (pkField == null) {
				throw new IllegalArgumentException("No primary key field found in table '" + table.getName() + '\'');
			}

			back.append("CREATE TRIGGER ").append(processID(triggerNameConverter.onUpdateName(table.getName(), field.getName())) + "\n");
			back.append("ON ").append(withSchema(table.getName())).append("\n");
			back.append("FOR UPDATE\nAS\n");
			back.append("    UPDATE ").append(withSchema(table.getName())).append(" SET ").append(processID(field.getName()));
			back.append(" = ").append(renderValue(onUpdate));
			back.append(" WHERE " + processID(pkField.getName()) + " = (SELECT " + processID(pkField.getName()) + " FROM inserted)");

			return back.toString();
		}

		return super.renderTriggerForField(triggerNameConverter, sequenceNameConverter, table, field);
	}

	@Override
	protected String renderAlterTableChangeColumnStatement(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field, RenderFieldOptions options) {
		StringBuilder current = new StringBuilder();

		current.append("ALTER TABLE ").append(withSchema(table.getName())).append(" ALTER COLUMN ");
		current.append(renderField(nameConverters, table, field, options));

		return current.toString();
	}

	@Override
	protected List<String> renderAlterTableAddColumn(NameConverters nameConverters, DDLTable table, DDLField field) {
        final TriggerNameConverter triggerNameConverter = nameConverters.getTriggerNameConverter();
        final SequenceNameConverter sequenceNameConverter = nameConverters.getSequenceNameConverter();

        List<String> back = new ArrayList<String>();

		back.add("ALTER TABLE " + withSchema(table.getName()) + " ADD " + renderField(nameConverters, table, field, new RenderFieldOptions(true, true)));

		String function = renderFunctionForField(triggerNameConverter, table, field);
		if (function != null) {
			back.add(function);
		}

		String trigger = renderTriggerForField(triggerNameConverter, sequenceNameConverter, table, field);
		if (trigger != null) {
			back.add(trigger);
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
	@SuppressWarnings("unused")
	public synchronized <T> T insertReturningKey(EntityManager manager, Connection conn, Class<T> pkType, String pkField,
			boolean pkIdentity, String table, DBParam... params) throws SQLException {
		boolean identityInsert = false;
		StringBuilder sql = new StringBuilder();

		if (pkIdentity) {
			for (DBParam param : params) {
				if (param.getField().trim().equalsIgnoreCase(pkField)) {
					identityInsert = true;
					sql.append("SET IDENTITY_INSERT ").append(withSchema(table)).append(" ON\n");
					break;
				}
			}
		}

		sql.append("INSERT INTO ").append(withSchema(table));

		if (params.length > 0) {
			sql.append(" (");
			for (DBParam param : params) {
				sql.append(processID(param.getField()));
				sql.append(',');
			}
			sql.setLength(sql.length() - 1);

			sql.append(") VALUES (");

			for (DBParam param : params) {
				sql.append("?,");
			}
			sql.setLength(sql.length() - 1);

			sql.append(")");
		} else {
			sql.append(" DEFAULT VALUES");
		}

		if (identityInsert) {
			sql.append("\nSET IDENTITY_INSERT ").append(processID(table)).append(" OFF");
		}

		T back = executeInsertReturningKey(manager, conn, pkType, pkField, sql.toString(), params);

		return back;
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}
}
