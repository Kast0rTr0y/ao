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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.java.ao.schema.Case;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.ddl.SQLAction;

import net.java.ao.Common;
import net.java.ao.DBParam;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.EntityManager;
import net.java.ao.Query;
import net.java.ao.RawEntity;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;

import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 * @author Daniel Spiewak
 */
public final class OracleDatabaseProvider extends DatabaseProvider
{
    private static final int ORA_04080_TRIGGER_DOES_NOT_EXIST = 4080;
    private static final int ORA_02289_SEQUENCE_DOES_NOT_EXIST = 2289;

    public OracleDatabaseProvider(DisposableDataSource dataSource)
    {
        this(dataSource, null);
    }

    public OracleDatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        super(dataSource, schema, TypeManager.oracle());
    }

    @Override
    public String getSchema()
    {
        return isSchemaNotEmpty() ? Case.UPPER.apply(super.getSchema()) : null;
    }

    @Override
    public ResultSet getTables(Connection conn) throws SQLException
    {
        final DatabaseMetaData metaData = conn.getMetaData();
        final String schemaPattern = isSchemaNotEmpty() ? getSchema() : metaData.getUserName();
        return metaData.getTables(null, schemaPattern, "%", new String[]{"TABLE"});
    }

    @Override
    public ResultSet getSequences(Connection conn) throws SQLException
    {
        final DatabaseMetaData metaData = conn.getMetaData();
        final String schemaPattern = isSchemaNotEmpty() ? getSchema() : metaData.getUserName();
        return metaData.getTables(null, schemaPattern, "%", new String[]{"SEQUENCE"});
    }

    @Override
    public ResultSet getIndexes(Connection conn, String tableName) throws SQLException
    {
        final DatabaseMetaData metaData = conn.getMetaData();
        final String schemaPattern = isSchemaNotEmpty() ? getSchema() : metaData.getUserName();
        return conn.getMetaData().getIndexInfo(null, schemaPattern, tableName, false, true);
    }

    @Override
    public ResultSet getImportedKeys(Connection connection, String tableName) throws SQLException
    {
        final DatabaseMetaData metaData = connection.getMetaData();
        final String schemaPattern = isSchemaNotEmpty() ? getSchema() : metaData.getUserName();
        return metaData.getImportedKeys(null, schemaPattern, tableName);
    }

    @Override
    protected String renderQuerySelect(final Query query, final TableNameConverter converter, final boolean count)
    {
        StringBuilder sql = new StringBuilder();

        // see http://www.oracle.com/technetwork/issue-archive/2006/06-sep/o56asktom-086197.html

        if (Query.QueryType.SELECT.equals(query.getType()))
        {
            int offset = query.getOffset();
            int limit = query.getLimit();

            if (offset > 0)
            {
                sql.append("SELECT * FROM ( SELECT QUERY_INNER.*, ROWNUM ROWNUM_INNER FROM ( ");
            }
            else if (limit >= 0)
            {
                sql.append("SELECT * FROM ( ");
            }
        }

        sql.append(super.renderQuerySelect(query, converter, count));

        return sql.toString();
    }

    @Override
	protected String renderQueryLimit(Query query) {
        StringBuilder sql = new StringBuilder();

        if (Query.QueryType.SELECT.equals(query.getType()))
        {
            int offset = query.getOffset();
            int limit = query.getLimit();

            if (offset > 0 && limit >= 0)
            {
                sql.append(" ) QUERY_INNER WHERE ROWNUM <= ");
                sql.append(offset + limit);
                sql.append(" ) WHERE ROWNUM_INNER > ");
                sql.append(offset);
            }
            else if (offset > 0)
            {
                sql.append(" ) QUERY_INNER ) WHERE ROWNUM_INNER > ");
                sql.append(offset);
            }
            else if (limit >= 0)
            {
                sql.append(" ) WHERE ROWNUM <= ");
                sql.append(limit);
            }
        }

        return sql.toString();
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
            case Types.TIMESTAMP:
                Matcher matcher = Pattern.compile("'(.*)'.*").matcher(value);
                if (matcher.find()) {
                    value = matcher.group(1);
                }
            break;
        }

        return super.parseValue(type, value);
    }

    @Override
    protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
        return "CONSTRAINT " + uniqueNameConverter.getName(table.getName(), field.getName()) + " UNIQUE";
    }

	@Override
	protected String getDateFormat() {
		return "dd-MMM-yy hh:mm:ss.SSS a";
	}

    @Override
    protected SQLAction renderAlterTableAddColumnStatement(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        String addStmt = "ALTER TABLE " + withSchema(table.getName()) + " ADD (" + renderField(nameConverters, table, field, new RenderFieldOptions(true, true, true)) + ")";
        return SQLAction.of(addStmt);
    }

    @Override
    protected Iterable<SQLAction> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
        final UniqueNameConverter uniqueNameConverter = nameConverters.getUniqueNameConverter();
        final ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        if(!oldField.getType().getLogicalType().equals(field.getType().getLogicalType()))
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" MODIFY (").append(processID(field.getName())).append(" ").append(renderFieldType(field)).append(")")));
        }

        if (oldField.isNotNull() && !field.isNotNull())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" MODIFY (").append(processID(field.getName())).append(" NULL)")));
        }

        if (!oldField.isNotNull() && field.isNotNull())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" MODIFY (").append(processID(field.getName())).append(" NOT NULL)")));
        }

        if (!Objects.equal(oldField.getDefaultValue(), field.getDefaultValue()))
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" MODIFY (").append(processID(field.getName())).append(" DEFAULT ").append(renderValue(field.getDefaultValue())).append(")")));
        }

        if (oldField.isUnique() && !field.isUnique())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" DROP CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName()))));
        }

        if (!oldField.isUnique() && field.isUnique())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" ADD CONSTRAINT ").append(uniqueNameConverter.getName(table.getName(), field.getName())).append(" UNIQUE (").append(processID(field.getName())).append(")")));
        }

        return back.build();
    }

	@Override
	protected SQLAction renderAlterTableDropKey(DDLForeignKey key)
	{
		StringBuilder back = new StringBuilder("ALTER TABLE ");

		back.append(withSchema(key.getDomesticTable())).append(" DROP CONSTRAINT ").append(processID(key.getFKName()));

		return SQLAction.of(back);
	}

    @Override
    protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        return SQLAction.of(new StringBuilder().append("DROP INDEX ").append(withSchema(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField())))));
    }

    @Override
    protected SQLAction renderDropTableStatement(DDLTable table)
    {
        return SQLAction.of("DROP TABLE " + withSchema(table.getName()) + " PURGE");
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
    protected <T extends RawEntity<K>, K> K executeInsertReturningKey(EntityManager manager, Connection conn, 
                                                                      Class<T> entityType, Class<K> pkType,
                                                                      String pkField, String sql, DBParam... params) throws SQLException
    {
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            onSql(sql);
            stmt = conn.prepareStatement(sql, new String[]{pkField});
            K back = (K) setParameters(manager, stmt, params, pkField);

            stmt.executeUpdate();

            if (back == null)
            {
                res = stmt.getGeneratedKeys();
                if (res.next())
                {
                    back = typeManager.getType(pkType).getLogicalType().pullFromDatabase(null, res, pkType, 1);
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

    private Object setParameters(EntityManager manager, PreparedStatement stmt, DBParam[] params, String pkField) throws SQLException
    {
        Object back = null;
        int i = 0;
        for (; i < params.length; i++)
        {
            Object value = params[i].getValue();
            if (value instanceof RawEntity<?>)
            {
                value = Common.getPrimaryKeyValue((RawEntity<?>) value);
            }
            if (params[i].getField().equalsIgnoreCase(pkField))
            {
                back = value;
            }
            if (value == null)
            {
                putNull(stmt, i + 1);
            }
            else
            {
                TypeInfo<Object> type = (TypeInfo<Object>) getTypeManager().getType(value.getClass());
                type.getLogicalType().putToDatabase(manager, stmt, i + 1, value, type.getJdbcWriteType());
            }
        }
        return back;
    }

    @Override
    protected Iterable<SQLAction> renderAccessoriesForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        if (field.isAutoIncrement())
        {
            return ImmutableList.of(renderSequence(nameConverters, table, field).withUndoAction(renderDropSequence(nameConverters, table, field)),
                                    renderTrigger(nameConverters, table, field).withUndoAction(renderDropTrigger(nameConverters, table, field)));
        }
        return ImmutableList.of();
    }

    @Override
    protected Iterable<SQLAction> renderDropAccessoriesForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        if (field.isAutoIncrement())
        {
            return ImmutableList.of(renderDropTrigger(nameConverters, table, field),
                                    renderDropSequence(nameConverters, table, field));
        }
        return ImmutableList.of();
    }

    private SQLAction renderSequence(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return SQLAction.of("CREATE SEQUENCE " +
            withSchema(nameConverters.getSequenceNameConverter().getName(shorten(table.getName()), shorten(field.getName()))) +
            " INCREMENT BY 1 START WITH 1 NOMAXVALUE MINVALUE 1");
    }
    
    private SQLAction renderTrigger(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        StringBuilder back = new StringBuilder();

        back.append("CREATE TRIGGER ").append(withSchema(nameConverters.getTriggerNameConverter().autoIncrementName(table.getName(), field.getName())) +  '\n');
        back.append("BEFORE INSERT\n").append("    ON ").append(withSchema(table.getName())).append("   FOR EACH ROW\n");
        back.append("BEGIN\n");
        back.append("    SELECT ").append(withSchema(nameConverters.getSequenceNameConverter().getName(shorten(table.getName()), shorten(field.getName()))) + ".NEXTVAL");
        back.append(" INTO :NEW.").append(processID(field.getName())).append(" FROM DUAL;\nEND;");

        return SQLAction.of(back);
    }

    private SQLAction renderDropSequence(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return SQLAction.of("DROP SEQUENCE " +
            withSchema(nameConverters.getSequenceNameConverter().getName(shorten(table.getName()), shorten(field.getName()))));
    }
    
    private SQLAction renderDropTrigger(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return SQLAction.of("DROP TRIGGER " +
            withSchema(nameConverters.getTriggerNameConverter().autoIncrementName(table.getName(), field.getName())));
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
    public void putNull(PreparedStatement stmt, int index) throws SQLException
    {
        stmt.setString(index, null);
    }

    @Override
    public void putBoolean(PreparedStatement stmt, int index, boolean value) throws SQLException
    {
        stmt.setInt(index, value ? 1 : 0);
    }

    public static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "ACCESS", "ACCOUNT", "ACTIVATE", "ADD", "ADMIN", "ADVISE", "AFTER",
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
            "WRITEDOWN", "WRITEUP", "XID", "YEAR", "ZONE");
}