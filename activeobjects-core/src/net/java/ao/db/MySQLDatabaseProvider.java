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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.java.ao.DisposableDataSource;
import net.java.ao.DatabaseProvider;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

/**
 * @author Daniel Spiewak
 */
public final class MySQLDatabaseProvider extends DatabaseProvider {
	private static final Set<String> RESERVED_WORDS = new HashSet<String>() {
		{
			addAll(Arrays.asList("ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", 
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
					"WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"));
		}
	};

    public MySQLDatabaseProvider(DisposableDataSource dataSource)
    {
        super(dataSource, null);
    }

    @Override
    protected boolean considerPrecision(DDLField field) {
        switch (field.getType().getType()) {
            case Types.BOOLEAN:
            case Types.LONGVARCHAR:
                return false;
        }

        return super.considerPrecision(field);
    }

	@Override
	protected String convertTypeToString(DatabaseType<?> type) {
		switch (type.getType()) {
			case Types.CLOB:
			case Types.LONGVARCHAR:
				return "TEXT";
		}
		
		return super.convertTypeToString(type);
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

    protected List<String> renderAlterTableAddColumn(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        List<String> back = new ArrayList<String>();
        back.addAll(super.renderAlterTableAddColumn(nameConverters, table, field));
        if (field.isUnique())
        {
            back.add(alterAddUniqueConstraint(nameConverters, table, field));
        }

        String function = renderFunctionForField(nameConverters.getTriggerNameConverter(), table, field);
        if (function != null)
        {
            back.add(function);
        }

        final String trigger = renderTriggerForField(nameConverters.getTriggerNameConverter(), nameConverters.getSequenceNameConverter(), table, field);
        if (trigger != null)
        {
            back.add(trigger);
        }

        return back;
    }

    private String alterAddUniqueConstraint(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" ADD CONSTRAINT ").append(nameConverters.getUniqueNameConverter().getName(table.getName(), field.getName())).append(" UNIQUE (").append(processID(field.getName())).append(")").toString();
    }

    @Override
    protected List<String> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
        final List<String> back = new ArrayList<String>();

        final String trigger = getTriggerNameForField(nameConverters.getTriggerNameConverter(), table, oldField);
        if (trigger != null)
        {
            back.add(new StringBuilder().append("DROP TRIGGER ").append(processID(trigger)).toString());
        }

        final String function = getFunctionNameForField(nameConverters.getTriggerNameConverter(), table, oldField);
        if (function != null)
        {
            back.add(new StringBuilder().append("DROP FUNCTION ").append(processID(function)).toString());
        }

        back.add(renderAlterTableChangeColumnStatement(nameConverters, table, oldField, field, renderFieldOptionsInAlterColumn()));

        if (oldField.isUnique() && !field.isUnique())
        {
            back.add(new StringBuilder().append("ALTER TABLE ").append(withSchema(table.getName())).append(" DROP INDEX ").append(nameConverters.getUniqueNameConverter().getName(table.getName(), field.getName())).toString());
        }

        if (!oldField.isUnique() && field.isUnique())
        {
            back.add(alterAddUniqueConstraint(nameConverters, table, field));
        }

        final String toRenderFunction = renderFunctionForField(nameConverters.getTriggerNameConverter(), table, field);
        if (toRenderFunction != null)
        {
            back.add(toRenderFunction);
        }

        final String toRenderTrigger = renderTriggerForField(nameConverters.getTriggerNameConverter(), nameConverters.getSequenceNameConverter(), table, field);
        if (toRenderTrigger != null)
        {
            back.add(toRenderTrigger);
        }

        return back;
    }

    @Override
    protected String renderFieldType(DDLField field)
    {
        if (field.getType().getType() == Types.NUMERIC) // numeric is used by Oracle
        {
            field.setType(typeManager.getType(Types.INTEGER));
        }
        return super.renderFieldType(field);
    }

    @Override
	protected String renderCreateIndex(IndexNameConverter indexNameConverter, DDLIndex index) {
		StringBuilder back = new StringBuilder("CREATE INDEX ");
		back.append(processID(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField())))).append(" ON ");
		back.append(processID(index.getTable())).append('(').append(processID(index.getField()));
		
		if (index.getType().getType() == Types.CLOB || index.getType().getType() == Types.VARCHAR) {
			int defaultPrecision = index.getType().getDefaultPrecision();
			back.append('(').append(defaultPrecision > 0 ? defaultPrecision : 255).append(')');
		}
		back.append(')');
		
		return back.toString();
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
}
