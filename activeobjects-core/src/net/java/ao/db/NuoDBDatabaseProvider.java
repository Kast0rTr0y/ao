package net.java.ao.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import net.java.ao.ActiveObjectsException;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.types.TypeManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Types.OTHER;
import static net.java.ao.Common.closeQuietly;

/**
 * @author Philip Stoev
 */
public class NuoDBDatabaseProvider extends DatabaseProvider {
    public NuoDBDatabaseProvider(DisposableDataSource dataSource) {
        this(dataSource, null);
    }

    public NuoDBDatabaseProvider(DisposableDataSource dataSource, String schema) {
        super(dataSource, schema, TypeManager.nuodb());
    }

    /**
     * Workaround for DB-7451 transaction isolation level SERIALIZABLE will not show tables created or altered with
     * auto-commit off.
     *
     * @param conn The connection to use in retrieving the database tables.
     * @return
     * @throws SQLException
     */
    @Override
    public ResultSet getTables(Connection conn) throws SQLException {
        int transactionIsolation = conn.getTransactionIsolation();
        try {
            conn.setTransactionIsolation(TRANSACTION_READ_COMMITTED);
            return conn.getMetaData().getTables(null, getSchema(), null, new String[]{"TABLE"});
        } finally {
            conn.setTransactionIsolation(transactionIsolation);
        }
    }

    @Override
    protected String renderAutoIncrement() {
        return "GENERATED BY DEFAULT AS IDENTITY";
    }

    @Override
    protected RenderFieldOptions renderFieldOptionsInAlterColumn() {
        return new RenderFieldOptions(false, false, true, true);
    }

    @Override
    protected Iterable<SQLAction> renderAlterTableChangeColumn(
            NameConverters nameConverters, DDLTable table, DDLField oldField,
            DDLField field) {
        final ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        back.addAll(renderDropAccessoriesForField(nameConverters, table, oldField));

        //Store if the field wants to be auto-increment, but turn it off.  NuoDB does not support adding auto-increment on existing columns.
        boolean needsAutoIncrement = field.isAutoIncrement();
        field.setAutoIncrement(false);

        //If we are changing the JDBC Type, we need to drop the primary index
        if (oldField.isPrimaryKey() && oldField.getJdbcType() != field.getJdbcType()) {
            back.add(findAndRenderDropUniqueIndex(nameConverters, table, field, true));
        }

        //Add alter statements for everything but unique.
        back.add(renderAlterTableChangeColumnStatement(nameConverters, table, oldField, field, new RenderFieldOptions(false, true, true)));

        //Need to drop default if necessary
        if (oldField.getDefaultValue() != null && field.getDefaultValue() == null) {
            back.add(SQLAction.of("ALTER TABLE " + withSchema(table.getName()) + " ALTER " + field.getName() + " DROP DEFAULT"));
        }

        //Need to drop null if necessary
        if (oldField.isNotNull() && !field.isNotNull()) {
            back.add(SQLAction.of("ALTER TABLE " + withSchema(table.getName()) + " ALTER " + field.getName() + " NULL"));
        }

        if (oldField.isUnique() && !field.isUnique()) {
            // SchemaReader filters out unique indices; so use the expected name patterns to try to find it, and drop it
            back.add(findAndRenderDropUniqueIndex(nameConverters, table, field, false));
        }

        if (!oldField.isUnique() && field.isUnique()) {
            back.add(renderUniqueIndex(nameConverters.getIndexNameConverter(), table.getName(), field.getName()));
        }

        if (field.isPrimaryKey() && oldField.getJdbcType() != field.getJdbcType()) {
            //If the type changed, and this field is the primary key, we need to redefine it.
            //alter table AO_000000_ENTITY add primary key (id) ;
            back.add(SQLAction.of("ALTER TABLE " + withSchema(table.getName()) + " ADD PRIMARY KEY (" + field.getName() + ")"));
        }

        back.addAll(renderAccessoriesForField(nameConverters, table, field));

        return back.build();
    }

    /**
     * Drop indices on field being altered.
     */
    @Override
    protected Iterable<SQLAction> renderDropAccessoriesForField(
            final NameConverters nameConverters,
            final DDLTable table,
            final DDLField field
    ) {
        return Stream.of(table.getIndexes())
                .filter(index -> containsFiled(index, field))
                .map(index -> renderDropIndex(nameConverters.getIndexNameConverter(), index))
                .collect(Collectors.toList());
    }

    /**
     * Create indices on field being altered.
     */
    @Override
    protected Iterable<SQLAction> renderAccessoriesForField(
            final NameConverters nameConverters,
            final DDLTable table,
            final DDLField field
    ) {
        return Stream.of(table.getIndexes())
                .filter(index -> containsFiled(index, field))
                .map(index -> renderCreateIndex(nameConverters.getIndexNameConverter(), index))
                .collect(Collectors.toList());
    }

    private boolean containsFiled(final DDLIndex index, final DDLField field) {
        return Stream.of(index.getFields())
                .map(DDLIndexField::getFieldName)
                .anyMatch(indexFieldName -> indexFieldName.equals(field.getName()));
    }

    private SQLAction findAndRenderDropUniqueIndex(
            NameConverters nameConverters, DDLTable table, DDLField field,
            boolean isPrimary) {
        String indexName = findUniqueIndex(table.getName(), field.getName(), isPrimary);
        if (indexName == null || indexName.isEmpty()) {
            logger.error("Unable to find unique index for field {} in table {}", field.getName(), table.getName());
        }

        DDLIndex index = DDLIndex.builder()
                .field(DDLIndexField.builder().fieldName(field.getName()).build())
                .table(table.getName())
                .indexName(indexName)
                .build();

        SQLAction renderDropIndex = renderDropIndex(nameConverters.getIndexNameConverter(), index);
        return renderDropIndex;
    }

    private String findUniqueIndex(String table, String field, boolean returnPrimary) {

        Connection connection = null;
        try {
            connection = getConnection();
            ResultSet indexes = getIndexes(connection, table);
            while (indexes.next()) {
                if (field.equalsIgnoreCase(indexes.getString("COLUMN_NAME"))) {
                    String name = indexes.getString("INDEX_NAME");
                    if (returnPrimary == name.contains("PRIMARY")) {
                        return indexes.getString("INDEX_NAME");
                    }
                }
            }
            return null;
        } catch (SQLException e) {
            throw new ActiveObjectsException(e);
        } finally {
            closeQuietly(connection);
        }
    }

    @Override
    protected SQLAction renderCreateIndex(IndexNameConverter indexNameConverter, DDLIndex index) {
        String statement = "CREATE INDEX " + index.getIndexName()
                + " ON " + withSchema(index.getTable()) +
                Stream.of(index.getFields())
                        .map(DDLIndexField::getFieldName)
                        .map(this::processID)
                        .collect(Collectors.joining(",", "(", ")"));

        return SQLAction.of(statement);
    }

    private SQLAction renderUniqueIndex(IndexNameConverter indexNameConverter, String table, String field) {
        return SQLAction.of("CREATE UNIQUE INDEX " + indexNameConverter.getName(shorten(table), shorten(field))
                + " ON " + withSchema(table) + "(" + processID(field) + ")");
    }

    @Override
    protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter,
                                        DDLIndex index) {
        final String indexName = index.getIndexName();
        final String tableName = index.getTable();

        if (hasIndex(tableName, indexName)) {
            return SQLAction.of("DROP INDEX " + withSchema("\"" + indexName + "\""));
        } else {
            return null;
        }
    }

    @Override
    protected String renderConstraintsForTable(UniqueNameConverter uniqueNameConverter, DDLTable table) {
        StringBuilder back = new StringBuilder();
        for (DDLForeignKey key : table.getForeignKeys()) {
            back.append("    ").append(renderForeignKey(key));
            if (!disableForeignKey(key)) {
                back.append(",");
            }
            back.append("\n");
        }
        return back.toString();
    }

    /**
     * Foreign keys which reference primary table aren't supported because of a open issue and are rendered in
     * commentary
     * block.
     *
     * @param key The database-agnostic foreign key representation.
     * @return
     */
    @Override
    protected String renderForeignKey(DDLForeignKey key) {
        StringBuilder back = new StringBuilder();
        if (disableForeignKey(key)) {
            back.append("/* DISABLE ");
        }
        back.append("FOREIGN KEY (").append(processID(key.getField())).append(") REFERENCES ");
        back.append(withSchema(key.getTable())).append('(').append(processID(key.getForeignField())).append(")");
        if (disableForeignKey(key)) {
            back.append(" */");
        }
        return back.toString();
    }

    private boolean disableForeignKey(DDLForeignKey key) {
        return key.getTable().equals(key.getDomesticTable());
    }

    @Override
    protected SQLAction renderAlterTableDropKey(DDLForeignKey key) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE ").append(withSchema(key.getDomesticTable()));
        sql.append(" DROP FOREIGN KEY (").append(processID(key.getField())).append(") REFERENCES ");
        sql.append(withSchema(key.getTable()));
        return SQLAction.of(sql);
    }

    @Override
    protected void loadQuoteString() {
        quoteRef.set("`");
    }

    @Override
    public Object parseValue(int type, String value) {
        if (value != null) {
            Matcher matcher = Pattern.compile("'(.*)'.*").matcher(value);
            if (matcher.find()) {
                value = matcher.group(1);
            }
            if (value.isEmpty()) value = null;
        }
        return super.parseValue(type, value);
    }


    @Override
    protected Set<String> getReservedWords() {
        return RESERVED_WORDS;
    }

    /**
     * The sql type argument is mandatory in JDBC for some reason, but is ignored by NuoDB JDBC,
     * so any integer value can be used.
     *
     * @param stmt  The statement in which to store the <code>NULL</code> value.
     * @param index The index of the parameter which should be assigned <code>NULL</code>.
     * @throws SQLException
     */
    @Override
    public void putNull(PreparedStatement stmt, int index) throws SQLException {
        stmt.setNull(index, OTHER);
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
