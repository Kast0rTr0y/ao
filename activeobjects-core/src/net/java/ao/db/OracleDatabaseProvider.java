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

import net.java.ao.DisposableDataSource;
import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.Query;
import net.java.ao.RawEntity;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 * @author Daniel Spiewak
 */
public class OracleDatabaseProvider extends DatabaseProvider {
	private static final Set<String> RESERVED_WORDS = new HashSet<String>() {
		{
			addAll(Arrays.asList("ACCESS", "ACCOUNT", "ACTIVATE", "ADD", "ADMIN", "ADVISE", "AFTER",
					"ALL", "ALL_ROWS", "ALLOCATE", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE",
					"ARCHIVELOG", "ARRAY", "AS", "ASC", "AT", "AUDIT", "AUTHENTICATED", "AUTHORIZATION",
					"AUTOEXTEND", "AUTOMATIC", "BACKUP", "BECOME", "BEFORE", "BEGIN", "BETWEEN",
					"BFILE", "BITMAP", "BLOB", "BLOCK", "BODY", "BY", "CACHE", "CACHE_INSTANCES",
					"CANCEL", "CASCADE", "CAST", "CFILE", "CHAINED", "CHANGE", "CHAR", "CHAR_CS",
					"CHARACTER", "CHECK", "CHECKPOINT", "CHOOSE", "CHUNK", "CLEAR", "CLOB", "CLONE",
					"CLOSE", "CLOSE_CACHED_OPEN_CURSORS", "CLUSTER", "COALESCE", "COLUMN", "COLUMNS",
					"COMMENT", "COMMIT", "COMMITTED", "COMPATIBILITY", "COMPILE", "COMPLETE",
					"COMPOSITE_LIMIT", "COMPRESS", "COMPUTE", "CONNECT", "CONNECT_TIME", "CONSTRAINT",
					"CONSTRAINTS", "CONTENTS", "CONTINUE", "CONTROLFILE", "CONVERT", "COST",
					"CPU_PER_CALL", "CPU_PER_SESSION", "CREATE", "CURRENT", "CURRENT_SCHEMA",
					"CURREN_USER", "CURSOR", "CYCLE", "DANGLING", "DATABASE", "DATAFILE", "DATAFILES",
					"DATAOBJNO", "DATE", "DBA", "DBHIGH", "DBLOW", "DBMAC", "DEALLOCATE", "DEBUG",
					"DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DEGREE",
					"DELETE", "DEREF", "DESC", "DIRECTORY", "DISABLE", "DISCONNECT", "DISMOUNT",
					"DISTINCT", "DISTRIBUTED", "DML", "DOUBLE", "DROP", "DUMP", "EACH", "ELSE",
					"ENABLE", "END", "ENFORCE", "ENTRY", "ESCAPE", "EXCEPT", "EXCEPTIONS", "EXCHANGE",
					"EXCLUDING", "EXCLUSIVE", "EXECUTE", "EXISTS", "EXPIRE", "EXPLAIN", "EXTENT",
					"EXTENTS", "EXTERNALLY", "FAILED_LOGIN_ATTEMPTS", "FALSE", "FAST", "FILE",
					"FIRST_ROWS", "FLAGGER", "FLOAT", "FLOB", "FLUSH", "FOR", "FORCE", "FOREIGN",
					"FREELIST", "FREELISTS", "FROM", "FULL", "FUNCTION", "GLOBAL", "GLOBALLY",
					"GLOBAL_NAME", "GRANT", "GROUP", "GROUPS", "HASH", "HASHKEYS", "HAVING", "HEADER",
					"HEAP", "IDENTIFIED", "IDGENERATORS", "IDLE_TIME", "IF", "IMMEDIATE", "IN",
					"INCLUDING", "INCREMENT", "INDEX", "INDEXED", "INDEXES", "INDICATOR",
					"IND_PARTITION", "INITIAL", "INITIALLY", "INITRANS", "INSERT", "INSTANCE",
					"INSTANCES", "INSTEAD", "INT", "INTEGER", "INTERMEDIATE", "INTERSECT", "INTO",
					"IS", "ISOLATION", "ISOLATION_LEVEL", "KEEP", "KEY", "KILL", "LABEL", "LAYER",
					"LESS", "LEVEL", "LIBRARY", "LIKE", "LIMIT", "LINK", "LIST", "LOB", "LOCAL",
					"LOCK", "LOCKED", "LOG", "LOGFILE", "LOGGING", "LOGICAL_READS_PER_CALL",
					"LOGICAL_READS_PER_SESSION", "LONG", "MANAGE", "MASTER", "MAX", "MAXARCHLOGS",
					"MAXDATAFILES", "MAXEXTENTS", "MAXINSTANCES", "MAXLOGFILES", "MAXLOGHISTORY",
					"MAXLOGMEMBERS", "MAXSIZE", "MAXTRANS", "MAXVALUE", "MIN", "MEMBER", "MINIMUM",
					"MINEXTENTS", "MINUS", "MINVALUE", "MLSLABEL", "MLS_LABEL_FORMAT", "MODE",
					"MODIFY", "MOUNT", "MOVE", "MTS_DISPATCHERS", "MULTISET", "NATIONAL", "NCHAR",
					"NCHAR_CS", "NCLOB", "NEEDED", "NESTED", "NETWORK", "NEW", "NEXT",
					"NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCYCLE", "NOFORCE",
					"NOLOGGING", "NOMAXVALUE", "NOMINVALUE", "NONE", "NOORDER", "NOOVERRIDE",
					"NOPARALLEL", "NOPARALLEL", "NOREVERSE", "NORMAL", "NOSORT", "NOT", "NOTHING",
					"NOWAIT", "NULL", "NUMBER", "NUMERIC", "NVARCHAR2", "OBJECT", "OBJNO",
					"OBJNO_REUSE", "OF", "OFF", "OFFLINE", "OID", "OIDINDEX", "OLD", "ON", "ONLINE",
					"ONLY", "OPCODE", "OPEN", "OPTIMAL", "OPTIMIZER_GOAL", "OPTION", "OR", "ORDER",
					"ORGANIZATION", "OSLABEL", "OVERFLOW", "OWN", "PACKAGE", "PARALLEL", "PARTITION",
					"PASSWORD", "PASSWORD_GRACE_TIME", "PASSWORD_LIFE_TIME", "PASSWORD_LOCK_TIME",
					"PASSWORD_REUSE_MAX", "PASSWORD_REUSE_TIME", "PASSWORD_VERIFY_FUNCTION",
					"PCTFREE", "PCTINCREASE", "PCTTHRESHOLD", "PCTUSED", "PCTVERSION", "PERCENT",
					"PERMANENT", "PLAN", "PLSQL_DEBUG", "POST_TRANSACTION", "PRECISION", "PRESERVE",
					"PRIMARY", "PRIOR", "PRIVATE", "PRIVATE_SGA", "PRIVILEGE", "PRIVILEGES",
					"PROCEDURE", "PROFILE", "PUBLIC", "PURGE", "QUEUE", "QUOTA", "RANGE", "RAW",
					"RBA", "READ", "READUP", "REAL", "REBUILD", "RECOVER", "RECOVERABLE", "RECOVERY",
					"REF", "REFERENCES", "REFERENCING", "REFRESH", "RENAME", "REPLACE", "RESET",
					"RESETLOGS", "RESIZE", "RESOURCE", "RESTRICTED", "RETURN", "RETURNING", "REUSE",
					"REVERSE", "REVOKE", "ROLE", "ROLES", "ROLLBACK", "ROW", "ROWID", "ROWNUM",
					"ROWS", "RULE", "SAMPLE", "SAVEPOINT", "SB4", "SCAN_INSTANCES", "SCHEMA", "SCN",
					"SCOPE", "SD_ALL", "SD_INHIBIT", "SD_SHOW", "SEGMENT", "SEG_BLOCK", "SEG_FILE",
					"SELECT", "SEQUENCE", "SERIALIZABLE", "SESSION", "SESSION_CACHED_CURSORS",
					"SESSIONS_PER_USER", "SET", "SHARE", "SHARED", "SHARED_POOL", "SHRINK", "SIZE",
					"SKIP", "SKIP_UNUSABLE_INDEXES", "SMALLINT", "SNAPSHOT", "SOME", "SORT",
					"SPECIFICATION", "SPLIT", "SQL_TRACE", "STANDBY", "START", "STATEMENT_ID",
					"STATISTICS", "STOP", "STORAGE", "STORE", "STRUCTURE", "SUCCESSFUL", "SWITCH",
					"SYS_OP_ENFORCE_NOT_NULL$", "SYS_OP_NTCIMG$", "SYNONYM", "SYSDATE", "SYSDBA",
					"SYSOPER", "SYSTEM", "TABLE", "TABLES", "TABLESPACE", "TABLESPACE_NO", "TABNO",
					"TEMPORARY", "THAN", "THE", "THEN", "THREAD", "TIMESTAMP", "TIME", "TO",
					"TOPLEVEL", "TRACE", "TRACING", "TRANSACTION", "TRANSITIONAL", "TRIGGER",
					"TRIGGERS", "TRUE", "TRUNCATE", "TX", "TYPE", "UB2", "UBA", "UID", "UNARCHIVED",
					"UNDO", "UNION", "UNIQUE", "UNLIMITED", "UNLOCK", "UNRECOVERABLE", "UNTIL",
					"UNUSABLE", "UNUSED", "UPDATABLE", "UPDATE", "USAGE", "USE", "USER", "USING",
					"VALIDATE", "VALIDATION", "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARYING",
					"VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WITHOUT", "WORK", "WRITE",
					"WRITEDOWN", "WRITEUP", "XID", "YEAR", "ZONE"));
		}
	};
    
    private static final int ORA_04080_TRIGGER_DOES_NOT_EXIST = 4080;
    private static final int ORA_02289_SEQUENCE_DOES_NOT_EXIST = 2289;

    public OracleDatabaseProvider(DisposableDataSource dataSource)
    {
        super(dataSource, null);
    }

    @Override
	public void setQueryStatementProperties(Statement stmt, Query query) throws SQLException {
		int limit = query.getLimit();
		if (limit >= 0) {
			stmt.setFetchSize(query.getOffset() + limit);
			stmt.setMaxRows(query.getOffset() + limit);
		}
	}


	@Override
	public void setQueryResultSetProperties(ResultSet res, Query query) throws SQLException {
		if (query.getOffset() > 0) {
			res.absolute(query.getOffset());
		}
	}

    @Override
    public ResultSet getTables(Connection conn) throws SQLException
    {
        DatabaseMetaData metaData = conn.getMetaData();
        return metaData.getTables(null, metaData.getUserName(), "%", new String[]{"TABLE"});
    }

    @Override
    public ResultSet getSequences(Connection conn) throws SQLException
    {
        DatabaseMetaData metaData = conn.getMetaData();
        return metaData.getTables(null, metaData.getUserName(), "%", new String[]{"SEQUENCE"});
    }

    @Override
    protected String convertTypeToString(DatabaseType<?> type)
    {
        switch (type.getType())
        {
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.SMALLINT:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return "NUMBER";
            case Types.LONGVARCHAR:
                return "CLOB";
        }
        return super.convertTypeToString(type);
    }

    @Override
    protected String renderFieldPrecision(DDLField field)
    {
        switch (field.getType().getType())
        {
            case Types.BIGINT:
                field.setPrecision(20);
                field.setScale(-1);
                break;
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.SMALLINT:
                field.setPrecision(11);
                field.setScale(-1);
                break;
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
                break;

        }
        return super.renderFieldPrecision(field);
    }

    @Override
	protected String renderQueryLimit(Query query) {
		return "";
	}

	@Override
	protected String renderAutoIncrement() {
		return "";
	}

    @Override
    public Object parseValue(int type, String value) {
        if (value == null || value.equals("") || value.equals("NULL"))
        {
            return null;
        }

        switch (type) {
            case Types.VARCHAR:
                Matcher matcher = Pattern.compile("'(.+)'.*").matcher(value);
                if (matcher.find()) {
                    value = matcher.group(1);
                }
            break;
        }

        return super.parseValue(type, value);
    }

    @Override
	protected String renderOnUpdate(DDLField field) {
		return "";
	}

	@Override
	protected String renderFunction(DatabaseFunction func) {
		switch (func) {
			case CURRENT_TIMESTAMP:
				return "SYSDATE";

			case CURRENT_DATE:
				return "SYSDATE";
		}

		return super.renderFunction(func);
	}

	@Override
	protected String getDateFormat() {
		return "dd-MMM-yy hh:mm:ss.SSS a";
	}

	@Override
	protected String renderTriggerForField(DDLTable table, DDLField field) {
		if (field.getOnUpdate() != null) {
			StringBuilder back = new StringBuilder();
			String value = renderValue(field.getOnUpdate());

			back.append("CREATE TRIGGER ").append(withSchema(table.getName() + '_' + field.getName() + "_onupdate") + '\n');
			back.append("BEFORE UPDATE\n").append("    ON ").append(withSchema(table.getName())).append("\n    FOR EACH ROW\n");
			back.append("BEGIN\n");
			back.append("    :NEW.").append(processID(field.getName())).append(" := ").append(value).append(";\nEND;");

			return back.toString();
		} else if (field.isAutoIncrement()) {
			StringBuilder back = new StringBuilder();

	        back.append("CREATE TRIGGER ").append(withSchema(table.getName() + '_' + field.getName() + "_autoinc") +  '\n');
	        back.append("BEFORE INSERT\n").append("    ON ").append(withSchema(table.getName())).append("   FOR EACH ROW\n");
	        back.append("BEGIN\n");
	        back.append("    SELECT ").append(withSchema(table.getName() + '_' + field.getName() + "_SEQ") + ".NEXTVAL");
	        back.append(" INTO :NEW.").append(processID(field.getName())).append(" FROM DUAL;\nEND;");

	        return back.toString();
		}

		return super.renderTriggerForField(table, field);
	}

	@Override
	protected String renderAlterTableChangeColumnStatement(DDLTable table, DDLField oldField, DDLField field) {
		StringBuilder current = new StringBuilder();
		current.append("ALTER TABLE ").append(withSchema(table.getName())).append(" MODIFY (");
		current.append(renderField(field)).append(')');
		return current.toString();
	}

	@Override
	protected String renderAlterTableDropKey(DDLForeignKey key) {
		StringBuilder back = new StringBuilder("ALTER TABLE ");

		back.append(processID(key.getDomesticTable())).append(" DROP CONSTRAINT ").append(processID(key.getFKName()));

		return back.toString();
	}

    @Override
    protected String renderDropTable(DDLTable table) {
        return "DROP TABLE " + withSchema(table.getName()) + " PURGE";
    }

    @Override
    public void handleUpdateError(String sql, SQLException e) throws SQLException
    {
        if (isDropTrigger(sql, e)
                || isDropSequence(sql, e))
        {
            logger.debug("Ignoring non-existant trigger for SQL <" + sql + ">", e);
            return;
        }

        super.handleUpdateError(sql, e);
    }

    private boolean isDropTrigger(String sql, SQLException e)
    {
        return e.getErrorCode() == ORA_04080_TRIGGER_DOES_NOT_EXIST && sql.startsWith("DROP");
    }

    private boolean isDropSequence(String sql, SQLException e)
    {
        return e.getErrorCode() == ORA_02289_SEQUENCE_DOES_NOT_EXIST && sql.startsWith("DROP");
    }

    @Override
	protected <T> T executeInsertReturningKey(EntityManager manager, Connection conn, Class<T> pkType, String pkField, String sql, DBParam... params) throws SQLException
    {
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            onSql(sql);
            stmt = conn.prepareStatement(sql, new String[]{pkField});
            T back = setParameters(stmt, params, pkField, pkType);

            stmt.executeUpdate();

            if (back == null)
            {
                res = stmt.getGeneratedKeys();
                if (res.next())
                {
                    back = TypeManager.getInstance().getType(pkType).pullFromDatabase(null, res, pkType, 1);
                }
            }
            return back;
        }
        finally
        {
            closeQuietly(res);
            closeQuietly(stmt);
        }
    }

    private <T> T setParameters(PreparedStatement stmt, DBParam[] params, String pkField, Class<T> pkType) throws SQLException
    {
        T back = null;
        int i = 0;
        for (; i < params.length; i++)
        {
            Object value = params[i].getValue();
            if (value instanceof RawEntity<?>)
            {
                value = Common.getPrimaryKeyValue((RawEntity<?>) value);
            }
            if (value instanceof URL)
            {
                stmt.setURL(i + 1, (URL) value);
            }
            else
            {
                stmt.setObject(i + 1, value);
            }

            if (params[i].getField().equalsIgnoreCase(pkField))
            {
                back = (T) value;
            }
        }
        return back;
    }

    @Override
	protected String[] renderTriggers(DDLTable table) {
        List<String> back = new ArrayList<String>();

        for (DDLField field : table.getFields()) {
            String trigger = renderTriggerForField(table, field);
            if (trigger != null) {
                back.add(trigger);
            }
        }

        return back.toArray(new String[back.size()]);
	}

	@Override
	protected String[] renderDropTriggers(DDLTable table) {
        List<String> back = new ArrayList<String>();

        for (DDLField field : table.getFields()) {
        	if (field.isAutoIncrement()) {
                StringBuilder seq = new StringBuilder();
                seq.append("DROP TRIGGER ").append(withSchema(table.getName() + '_' + field.getName() + "_autoinc"));
                back.add(seq.toString());
        	}
        }

        return back.toArray(new String[back.size()]);
    }

	@Override
	protected String[] renderDropSequences(DDLTable table) {
        List<String> back = new ArrayList<String>();

        for (DDLField field : table.getFields()) {
        	if (field.isAutoIncrement()) {
                StringBuilder seq = new StringBuilder();
                seq.append("DROP SEQUENCE ").append(withSchema(table.getName() + '_' + field.getName() + "_SEQ"));
                back.add(seq.toString());
        	}
        }

        return back.toArray(new String[back.size()]);
	}

	@Override
    protected String[] renderSequences(DDLTable table) {
        List<String> back = new ArrayList<String>();

        for (DDLField field : table.getFields()) {
        	if (field.isAutoIncrement()) {
                StringBuilder seq = new StringBuilder();
                seq.append("CREATE SEQUENCE ").append(withSchema(table.getName() + '_' + field.getName() + "_SEQ"));
                seq.append(" INCREMENT BY 1 START WITH 1 ");
                seq.append("NOMAXVALUE").append(" MINVALUE 1");
                back.add(seq.toString());
        	}
        }

        return back.toArray(new String[back.size()]);
	}

	@Override
	protected boolean shouldQuoteID(String id) {
        return !"*".equals(id);
	}

	@Override
	protected int getMaxIDLength() {
		return 30;
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}

	@Override
	public void putBoolean(PreparedStatement stmt, int index, boolean value) throws SQLException {
		stmt.setInt(index, value ? 1 : 0);
	}
}