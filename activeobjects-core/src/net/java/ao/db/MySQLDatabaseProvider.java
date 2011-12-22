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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import static com.google.common.collect.Iterables.concat;

import net.java.ao.schema.ddl.SQLAction;

import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.TypeManager;

/**
 * @author Daniel Spiewak
 */
public class MySQLDatabaseProvider extends DatabaseProvider
{
    public MySQLDatabaseProvider(DisposableDataSource dataSource)
    {
        super(dataSource, null, TypeManager.mysql());
    }

    @Override
	protected String renderAutoIncrement() {
		return "AUTO_INCREMENT";
	}
	
	@Override
	protected String renderAppend() {
		return "ENGINE=InnoDB";
	}

    @Override
    protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
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
    protected Iterable<SQLAction> renderAlterTableAddColumn(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        final Iterable<SQLAction> back = super.renderAlterTableAddColumn(nameConverters, table, field);
        if (field.isUnique())
        {
            return concat(back, ImmutableList.of(alterAddUniqueConstraint(nameConverters, table, field)));
        }
        return back;
    }

    private SQLAction alterAddUniqueConstraint(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" ADD CONSTRAINT ").append(nameConverters.getUniqueNameConverter().getName(table.getName(), field.getName())).append(" UNIQUE (").append(processID(field.getName())).append(")"));
    }

    @Override
    protected Iterable<SQLAction> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
        final ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        SQLAction dropTrigger = renderDropTriggerForField(nameConverters, table, oldField);
        if (dropTrigger != null)
        {
            back.add(dropTrigger);
        }

        SQLAction dropFunction = renderDropFunctionForField(nameConverters, table, oldField);
        if (dropFunction != null)
        {
            back.add(dropFunction);
        }

        back.add(renderAlterTableChangeColumnStatement(nameConverters, table, oldField, field, renderFieldOptionsInAlterColumn()));

        if (oldField.isUnique() && !field.isUnique())
        {
            back.add(SQLAction.of(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" DROP INDEX ").append(nameConverters.getUniqueNameConverter().getName(table.getName(), field.getName()))));
        }

        if (!oldField.isUnique() && field.isUnique())
        {
            back.add(alterAddUniqueConstraint(nameConverters, table, field));
        }
        
        SQLAction toRenderFunction = renderFunctionForField(nameConverters, table, field);
        if (toRenderFunction != null)
        {
            back.add(toRenderFunction);
        }

        SQLAction toRenderTrigger = renderTriggerForField(nameConverters, table, field);
        if (toRenderTrigger != null)
        {
            back.add(toRenderTrigger);
        }

        return back.build();
    }

    @Override
	protected SQLAction renderCreateIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
		StringBuilder back = new StringBuilder("CREATE INDEX ");
		back.append(processID(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField())))).append(" ON ");
		back.append(processID(index.getTable())).append('(').append(processID(index.getField())).append(')');
		
		return SQLAction.of(back);
	}

    public void putNull(PreparedStatement stmt, int index) throws SQLException
    {
        stmt.setString(index, null);
    }

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}

    @Override
    public boolean isCaseSensetive()
    {
        return FileSystemUtils.isCaseSensitive();
    }

    private static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
            "ASENSITIVE", "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB",
            "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR",
            "CHARACTER", "CHECK", "COLLATE", "COLUMN", "COLUMNS", "CONDITION",
            "CONNECTION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
            "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
            "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
            "DELAYED", "DELETE", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT",
            "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE",
            "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT", "EXPLAIN", "FALSE",
            "FETCH", "FIELDS", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE",
            "FOREIGN", "FROM", "FULLTEXT", "GOTO", "GRANT", "GROUP", "HAVING",
            "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
            "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE",
            "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER",
            "INTERVAL", "INTO", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL",
            "LABEL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINES", "LOAD",
            "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT",
            "LOOP", "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT",
            "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
            "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON",
            "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER",
            "OUTFILE", "PRECISION", "PRIMARY", "PRIVILEGES", "PROCEDURE", "PURGE",
            "READ", "READS", "REAL", "REFERENCES", "REGEXP", "RELEASE", "RENAME",
            "REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE", "RIGHT",
            "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE",
            "SEPARATOR", "SET", "SHOW", "SMALLINT", "SONAME", "SPATIAL", "SPECIFIC",
            "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
            "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING",
            "STRAIGHT_JOIN", "TABLE", "TABLES", "TERMINATED", "THEN", "TINYBLOB",
            "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO",
            "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "UPGRADE", "USAGE",
            "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES",
            "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN", "WHERE",
            "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL");
}
