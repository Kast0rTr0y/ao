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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.java.ao.schema.ddl.SQLAction;

import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.Query;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.types.TypeManager;

/**
 * @author Daniel Spiewak
 */
abstract class DerbyDatabaseProvider extends DatabaseProvider
{
    DerbyDatabaseProvider(DisposableDataSource dataSource)
    {
        super(dataSource, null, TypeManager.derby());
    }

    @Override
    public String renderMetadataQuery(final String tableName)
    {
        return "SELECT * (SELECT * FROM " + withSchema(tableName) + ") WHERE ROWNUM <= 1";
    }

    @Override
	public void setQueryStatementProperties(Statement stmt, Query query) throws SQLException {
		int limit = query.getLimit();
		
		if (limit >= 0) {
			stmt.setFetchSize(limit);
			stmt.setMaxRows(limit);
		}
	}
	
	@Override
	public void setQueryResultSetProperties(ResultSet res, Query query) throws SQLException {
		if (query.getOffset() > 0) {
			res.absolute(query.getOffset());
		}
	}
	
	@Override
	public ResultSet getTables(Connection conn) throws SQLException {
		return conn.getMetaData().getTables("APP", getSchema(), null, new String[] {"TABLE"});
	}
	
	@Override
	public Object parseValue(int type, String value)
    {
        if (value == null || value.equals("") || value.equals("NULL"))
        {
            return null;
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
        }

        return super.parseValue(type, value);
    }
	
	@Override
	protected void setPostConnectionProperties(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("SET SCHEMA app");
		stmt.close();
	}
	
	@Override
	protected String renderQueryLimit(Query query) {
		return "";
	}
	
	@Override
	protected String renderAutoIncrement() {
		return "GENERATED BY DEFAULT AS IDENTITY";
	}
	
	@Override
    public Object handleBlob(ResultSet res, Class<?> type, String field) throws SQLException
    {
        final Blob blob = res.getBlob(field);
        if (type.equals(InputStream.class))
        {
            return new ByteArrayInputStream(blob.getBytes(1, (int) blob.length()));
        }
        else if (type.equals(byte[].class))
        {
            return blob.getBytes(1, (int) blob.length());
        }
        else
        {
            return null;
        }
    }

	@Override
	protected Iterable<SQLAction> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
	{
		logger.warn("Derby doesn't support CHANGE TABLE statements!");
		logger.warn("Migration may not be entirely in sync as a result!");
		
		return ImmutableList.of();
	}
	
	@Override
	protected Iterable<SQLAction> renderAlterTableDropColumn(NameConverters nameConverters, DDLTable table, DDLField field)
	{
		System.err.println("WARNING: Derby doesn't support ALTER TABLE DROP COLUMN statements");
		
        return ImmutableList.of();
	}
	
	@Override
	protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
	{
	    return SQLAction.of("DROP INDEX " + processID(getExistingIndexName(indexNameConverter,index)));
	}

	@Override
	protected Set<String> getReservedWords() {
		return RESERVED_WORDS;
	}
	
	@Override
	public boolean isCaseSensitive() {
		return false;
	}

    private static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS",
            "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIT",
            "BOOLEAN", "BOTH", "BY", "CALL", "CASCADE", "CASCADED", "CASE", "CAST",
            "CHAR", "CHARACTER", "CHECK", "CLOSE", "COLLATE", "COLLATION", "COLUMN",
            "COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE",
            "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CURRENT", "CURRENT_DATE",
            "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DEALLOCATE",
            "DEC", "DECIMAL", "DECLARE", "DEFERRABLE", "DEFERRED", "DELETE", "DESC",
            "DESCRIBE", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOUBLE", "DROP", "ELSE",
            "END", "ENDEXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS",
            "EXPLAIN", "EXTERNAL", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN",
            "FOUND", "FROM", "FULL", "FUNCTION", "GET", "GET_CURRENT_CONNECTION", "GLOBAL",
            "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE",
            "IN", "INDICATOR", "INITIALLY", "INNER", "INOUT", "INPUT", "INSENSITIVE",
            "INSERT", "INT", "INTEGER", "INTERSECT", "INTO", "IS", "ISOLATION", "JOIN",
            "KEY", "LAST", "LEFT", "LIKE", "LONGINT", "LOWER", "LTRIM", "MATCH", "MAX",
            "MIN", "MINUTE", "NATIONAL", "NATURAL", "NCHAR", "NVARCHAR", "NEXT", "NO",
            "NOT", "NULL", "NULLIF", "NUMERIC", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR",
            "ORDER", "OUT", "OUTER", "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "PREPARE",
            "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC", "READ",
            "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK",
            "ROWS", "RTRIM", "SCHEMA", "SCROLL", "SECOND", "SELECT", "SESSION_USER", "SET",
            "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTR",
            "SUBSTRING", "SUM", "SYSTEM_USER", "TABLE", "TEMPORARY", "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION",
            "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USER", "USING",
            "VALUES", "VARCHAR", "VARYING", "VIEW", "WHENEVER", "WHERE", "WITH", "WORK",
            "WRITE", "XML", "XMLEXISTS", "XMLPARSE", "XMLSERIALIZE", "YEAR");
}
