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
package net.java.ao;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import net.java.ao.schema.Case;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.DDLValue;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.sql.SqlUtils;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.filter;

import static com.google.common.collect.Iterables.concat;

import static com.google.common.collect.Iterables.addAll;

import static com.google.common.collect.Sets.union;

import net.java.ao.schema.ddl.SQLAction;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static net.java.ao.Common.*;

/**
 * <p>The superclass parent of all <code>DatabaseProvider</code>
 * implementations.  Various implementations allow for an abstraction
 * around database-specific functionality (such as DDL).  DatabaseProvider(s)
 * also handle the creation of new {@link Connection} instances and
 * fully encapsulate the raw JDBC driver.  <i>Any</i> database-specific
 * code should be placed in the database provider, rather than embedded
 * within the API logic.</p>
 * <p/>
 * <p>This superclass contains a base-line, default implementation of most
 * database-specific methods, thus requiring a minimum of work to implement
 * a new database provider.  For the sake of sanity (read: mine), this
 * base-line implementation is basically specific to MySQL.  Thus any
 * DatabaseProvider implementations are really specifying the
 * <i>differences</i> between the database in question and MySQL.  To fully
 * utilize the default implementations provided in this class, this fact should
 * be kept in mind.</p>
 * <p/>
 * <p>This class also handles the implementation details required to ensure
 * that only one active {@link Connection} instance is available per thread.  This is
 * in fact a very basic (and naive) form of connection pooling. It should
 * <i>not</i> be relied upon for performance reasons.  Instead one should enable
 * connection pooling via the {@link javax.sql.DataSource} passed to instances. You can
 * also use the <em>net.java.ao.builder.EntityManagerBuilder</em> builder to make it
 * easier to configure the pooling. The purpose of the thread-locked connection pooling
 * in this class is to satisfy transactions with external SQL statements.</p>
 *
 * @author Daniel Spiewak
 */
public abstract class DatabaseProvider implements Disposable
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Logger sqlLogger = LoggerFactory.getLogger("net.java.ao.sql");

    private final Set<SqlListener> sqlListeners;

    private final ThreadLocal<Connection> transactionThreadLocal = new ThreadLocal<Connection>();
    private final DisposableDataSource dataSource;
    protected final TypeManager typeManager;

    private final String schema;

    private String quote;

    protected DatabaseProvider(DisposableDataSource dataSource, String schema, TypeManager typeManager)
    {
        this.dataSource = checkNotNull(dataSource);
        this.typeManager = typeManager;
        this.schema = isBlank(schema) ? null : schema; // can be null
        this.sqlListeners = new CopyOnWriteArraySet<SqlListener>();
        this.sqlListeners.add(new LoggingSqlListener(sqlLogger));
        loadQuoteString();
    }

    protected DatabaseProvider(DisposableDataSource dataSource, String schema)
    {
        this(dataSource, schema, new TypeManager.Builder().build());
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public String getSchema()
    {
        return schema;
    }

    private synchronized void loadQuoteString()
    {
        if (quote != null)
        {
            return;
        }

        Connection conn = null;
        try
        {
            conn = getConnection();
            if (conn == null)
            {
                throw new IllegalStateException("Could not get connection to load quote String");
            }
            quote = conn.getMetaData().getIdentifierQuoteString().trim();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Unable to query the database", e);
        }
        finally
        {
            closeQuietly(conn);
        }
    }

    /**
     * <p>Generates the DDL fragment required to specify an INTEGER field as
     * auto-incremented.  For databases which do not support such flags (which
     * is just about every database exception MySQL), <code>""</code> is an
     * acceptable return value.  This method should <i>never</i> return <code>null</code>
     * as it would cause the field rendering method to throw a {@link NullPointerException}.</p>
     */
    protected String renderAutoIncrement()
    {
        return "AUTO_INCREMENT";
    }

    /**
     * Top level delegating method for the process of rendering a database-agnostic
     * {@link DDLAction} into the database-specific SQL actions.  If the action is to
     * create an entity, then each of the SQL actions may have a corresponding undo
     * action to roll back creation of the entity if necessary.
     *
     * @param nameConverters
     * @param action The database-agnostic action to render.
     * @return An array of DDL statements specific to the database in question.
     * @see #renderTable(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable)
     * @see #renderFunctions(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderTriggers(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderSequences(net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderDropTriggers(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderDropFunctions(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderDropSequences(net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable)
     * @see #renderDropTableActions(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable)
     * @see #renderAlterTableAddColumn(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #renderAlterTableChangeColumn(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField, net.java.ao.schema.ddl.DDLField)
     * @see #renderDropColumnActions(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #renderAlterTableAddKey(DDLForeignKey)
     * @see #renderAlterTableDropKey(DDLForeignKey)
     */
    public final Iterable<SQLAction> renderAction(NameConverters nameConverters, DDLAction action)
    {
        switch (action.getActionType())
        {
            case CREATE:
                return renderCreateTableActions(nameConverters, action.getTable());

            case DROP:
                return renderDropTableActions(nameConverters, action.getTable());

            case ALTER_ADD_COLUMN:
                return renderAddColumnActions(nameConverters, action.getTable(), action.getField());

            case ALTER_CHANGE_COLUMN:
                return renderAlterTableChangeColumn(nameConverters, action.getTable(), action.getOldField(), action.getField());

            case ALTER_DROP_COLUMN:
                return renderDropColumnActions(nameConverters, action.getTable(), action.getField());

            case ALTER_ADD_KEY:
                return ImmutableList.of(renderAlterTableAddKey(action.getKey())
                                        .withUndoAction(renderAlterTableDropKey(action.getKey())));

            case ALTER_DROP_KEY:
                return ImmutableList.of(renderAlterTableDropKey(action.getKey()));

            case CREATE_INDEX:
                return ImmutableList.of(renderCreateIndex(nameConverters.getIndexNameConverter(), action.getIndex())
                                        .withUndoAction(renderDropIndex(nameConverters.getIndexNameConverter(), action.getIndex())));

            case DROP_INDEX:
                return ImmutableList.of(renderDropIndex(nameConverters.getIndexNameConverter(), action.getIndex()));
                
            case INSERT:
                return ImmutableList.of(renderInsert(action.getTable(), action.getValues()));
        }
        throw new IllegalArgumentException("unknown DDLAction type " + action.getActionType());
    }

    private Iterable<SQLAction> renderCreateTableActions(NameConverters nameConverters, DDLTable table)
    {
        ImmutableList.Builder<SQLAction> ret = ImmutableList.builder();
        
        ret.add(renderTable(nameConverters, table).withUndoAction(renderDropTableStatement(table)));
        
        ret.addAll(renderAccessories(nameConverters, table));

        for (DDLIndex index : table.getIndexes())
        {
            DDLAction newAction = new DDLAction(DDLActionType.CREATE_INDEX);
            newAction.setIndex(index);
            ret.addAll(renderAction(nameConverters, newAction));
        }
        
        return ret.build();
    }
    
    private Iterable<SQLAction> renderDropTableActions(NameConverters nameConverters, DDLTable table)
    {
        ImmutableList.Builder<SQLAction> ret = ImmutableList.builder();
        
        for (DDLIndex index : table.getIndexes())
        {
            final SQLAction sqlAction = renderDropIndex(nameConverters.getIndexNameConverter(), index);
            if (sqlAction != null)
            {
                ret.add(sqlAction);
            }
        }

        ret.addAll(renderDropAccessories(nameConverters, table));
        ret.add(renderDropTableStatement(table));
        
        return ret.build();
    }
    
    private Iterable<SQLAction> renderAddColumnActions(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        ImmutableList.Builder<SQLAction> ret = ImmutableList.builder();
        
        ret.addAll(renderAlterTableAddColumn(nameConverters, table, field));
        
        for (DDLIndex index : table.getIndexes())
        {
            if (index.getField().equals(field.getName()))
            {
                DDLAction newAction = new DDLAction(DDLActionType.CREATE_INDEX);
                newAction.setIndex(index);
                ret.addAll(renderAction(nameConverters, newAction));
            }
        }
        
        return ret.build();
    }
    
    private Iterable<SQLAction> renderDropColumnActions(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        ImmutableList.Builder<SQLAction> ret = ImmutableList.builder();
        
        for (DDLIndex index : table.getIndexes())
        {
            if (index.getField().equals(field.getName()))
            {
                DDLAction newAction = new DDLAction(DDLActionType.DROP_INDEX);
                newAction.setIndex(index);
                ret.addAll(renderAction(nameConverters, newAction));
            }
        }

        ret.addAll(renderAlterTableDropColumn(nameConverters, table, field));
        
        return ret.build();
    }
    
    /**
     * <p>Top level delegating method for rendering a database-agnostic
     * {@link Query} object into its (potentially) database-specific
     * query statement.  This method invokes the various <code>renderQuery*</code>
     * methods to construct its output, thus it is doubtful that any subclasses
     * will have to override it.  Rather, one of the delegate methods
     * should be considered.</p>
     * <p/>
     * <p>An example of a database-specific query rendering would be the
     * following <code>Query</code>:</p>
     * <p/>
     * <pre>Query.select().from(Person.class).limit(10)</pre>
     * <p/>
     * <p>On MySQL, this would render to <code>SELECT id FROM people LIMIT 10</code>
     * However, on SQL Server, this same Query would render as
     * <code>SELECT TOP 10 id FROM people</code></p>
     *
     * @param query The database-agnostic Query object to be rendered in a
     * potentially database-specific way.
     * @param converter Used to convert {@link Entity} classes into table names.
     * @param count If <code>true</code>, render the Query as a <code>SELECT COUNT(*)</code>
     * rather than a standard field-data query.
     * @return A syntactically complete SQL statement potentially specific to the
     *         database.
     * @see #renderQuerySelect(Query, TableNameConverter, boolean)
     * @see #renderQueryJoins(Query, TableNameConverter)
     * @see #renderQueryWhere(Query)
     * @see #renderQueryGroupBy(Query)
     * @see #renderQueryOrderBy(Query)
     * @see #renderQueryLimit(Query)
     */
    public String renderQuery(Query query, TableNameConverter converter, boolean count)
    {
        StringBuilder sql = new StringBuilder();

        sql.append(renderQuerySelect(query, converter, count));
        sql.append(renderQueryJoins(query, converter));
        sql.append(renderQueryWhere(query));
        sql.append(renderQueryGroupBy(query));
        sql.append(renderQueryOrderBy(query));
        sql.append(renderQueryLimit(query));

        return sql.toString();
    }

    /**
     * <p>Parses the database-agnostic <code>String</code> value relevant to the specified SQL
     * type in <code>int</code> form (as defined by {@link Types} and returns
     * the Java value which corresponds.  This method is completely database-agnostic, as are
     * all of all of its delegate methods.</p>
     * <p/>
     * <p><b>WARNING:</b> This method is being considered for removal to another
     * class (perhaps {@link TypeManager}?) as it is not a database-specific function and thus
     * confuses the purpose of this class.  Do not rely upon it heavily.  (better yet, don't rely on it
     * at all from external code.  It's not designed to be part of the public API)</p>
     *
     * @param type The JDBC integer type of the database field against which to parse the
     * value.
     * @param value The database-agnostic String value to parse into a proper Java object
     * with respect to the specified SQL type.
     * @return A Java value which corresponds to the specified String.
     */
    public Object parseValue(int type, String value)
    {
        if (value == null || value.equals("NULL"))
        {
            return null;
        }
        try
        {
            switch (type)
            {
                case Types.BIGINT:
                    return Long.parseLong(value);

                case Types.BIT:
                    try
                    {
                        return Byte.parseByte(value) != 0;
                    }
                    catch (Throwable t)
                    {
                        return Boolean.parseBoolean(value);
                    }
                case Types.BOOLEAN:
                    try
                    {
                        return Integer.parseInt(value) != 0;
                    }
                    catch (Throwable t)
                    {
                        return Boolean.parseBoolean(value);
                    }
                case Types.CHAR:
                    value.charAt(0);
                    break;

                case Types.DATE:
                    try
                    {
                        return new SimpleDateFormat(getDateFormat()).parse(value);
                    }
                    catch (ParseException e)
                    {
                        return null;
                    }

                case Types.DECIMAL:
                    return Double.parseDouble(value);

                case Types.DOUBLE:
                    return Double.parseDouble(value);

                case Types.FLOAT:
                    return Float.parseFloat(value);

                case Types.INTEGER:
                    return Integer.parseInt(value);

                case Types.NUMERIC:
                    return Integer.parseInt(value);

                case Types.REAL:
                    return Double.parseDouble(value);

                case Types.SMALLINT:
                    return Short.parseShort(value);

                case Types.TIMESTAMP:
                    try
                    {
                        return new SimpleDateFormat(getDateFormat()).parse(value);
                    }
                    catch (ParseException e)
                    {
                        return null;
                    }

                case Types.TINYINT:
                    return Short.parseShort(value);

                case Types.VARCHAR:
                    return value;
            }
        }
        catch (Throwable t)
        {
        }
        return null;
    }

    /**
     * <p>Allows the provider to set database-specific options on a
     * {@link Statement} instance prior to its usage in a SELECT
     * query.  This is to allow things like emulation of the
     * LIMIT feature on databases which don't support it within
     * the SQL implementation.</p>
     * <p/>
     * <p>This method is only called on SELECTs.</p>
     *
     * @param stmt The instance against which the properties
     * should be set.
     * @param query The query which is being executed against
     * the statement instance.
     */
    public void setQueryStatementProperties(Statement stmt, Query query) throws SQLException
    {
    }

    /**
     * Allows the provider to set database-specific options on a
     * {@link ResultSet} instance prior to its use by the library.
     * This allows for features such as row offsetting even on
     * databases that don't support it (such as Oracle, Derby,
     * etc).
     *
     * @param res The <code>ResultSet</code> to modify.
     * @param query The query instance which was run to produce
     * the result set.
     */
    public void setQueryResultSetProperties(ResultSet res, Query query) throws SQLException
    {
    }

    /**
     * <p>Returns a result set of all of the tables (and associated
     * meta) in the database.  The fields of the result set must
     * correspond with those specified in the
     * <code>DatabaseMetaData#getTables(String, String, String, String[])</code>
     * method.  In fact, the default implementation merely calls
     * this method passing <code>(null, null, "", null)</code>.
     * For databases (such as PostgreSQL) where this is unsuitable,
     * different parameters can be specified to the <code>getTables</code>
     * method in the override, or an entirely new implementation
     * written, as long as the result set corresponds in fields to
     * the JDBC spec.</p>
     *
     * @param conn The connection to use in retrieving the database tables.
     * @return A result set of tables (and meta) corresponding in fields
     *         to the JDBC specification.
     * @see java.sql.DatabaseMetaData#getTables(String, String, String, String[])
     */
    public ResultSet getTables(Connection conn) throws SQLException
    {
        return conn.getMetaData().getTables(null, schema, "%", new String[]{"TABLE"});
    }

    public ResultSet getSequences(Connection conn) throws SQLException
    {
        return conn.getMetaData().getTables(null, schema, "%", new String[]{"SEQUENCE"});
    }

    public ResultSet getIndexes(Connection conn, String tableName) throws SQLException
    {
        return conn.getMetaData().getIndexInfo(null, schema, tableName, false, false);
    }

    public ResultSet getImportedKeys(Connection connection, String tableName) throws SQLException
    {
        return connection.getMetaData().getImportedKeys(null, schema, tableName);
    }

    /**
     * <p>Renders the SELECT portion of a given {@link Query} instance in the
     * manner required by the database-specific SQL implementation.  Usually,
     * this is as simple as <code>"SELECT id FROM table"</code> or <code>"SELECT DISTINCT
     * * FROM table"</code>.  However, some databases require the limit and offset
     * parameters to be specified as part of the SELECT clause.  For example,
     * on HSQLDB, a Query for the "id" field limited to 10 rows would render
     * SELECT like this: <code>SELECT TOP 10 id FROM table</code>.</p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the SELECT properties.
     * @param converter The name converter to allow conversion of the query entity
     * interface into a proper table name.
     * @param count Whether or not the query should be rendered as a <code>SELECT COUNT(*)</code>.
     * @return The database-specific SQL rendering of the SELECT portion of the query.
     */
    protected String renderQuerySelect(Query query, TableNameConverter converter, boolean count)
    {
        StringBuilder sql = new StringBuilder();
        switch (query.getType())
        {
            case SELECT:
                sql.append("SELECT ");
                if (query.isDistinct())
                {
                    sql.append("DISTINCT ");
                }

                if (count)
                {
                    sql.append("COUNT(*)");
                }
                else
                {
                    sql.append(querySelectFields(query));
                }
                sql.append(" FROM ").append(queryTableName(query, converter));
                break;
        }

        return sql.toString();
    }

    protected final String queryTableName(Query query, TableNameConverter converter)
    {
        final String queryTable = query.getTable();
        final String tableName = queryTable != null ? queryTable : converter.getName(query.getTableType());

        final StringBuilder queryTableName = new StringBuilder().append(withSchema(tableName));
        if (query.getAlias(query.getTableType()) != null)
        {
            queryTableName.append(" ").append(query.getAlias(query.getTableType()));
        }
        return queryTableName.toString();
    }

    protected final String querySelectFields(final Query query)
    {
        return Joiner.on(',').join(transform(query.getFields(), new Function<String, String>()
        {
            @Override
            public String apply(String fieldName)
            {
                return withAlias(query, fieldName);
            }
        }));
    }

    private String withAlias(Query query, String field)
    {
        final StringBuilder withAlias = new StringBuilder();
        if (query.getAlias(query.getTableType()) != null)
        {
            withAlias.append(query.getAlias(query.getTableType())).append(".");
        }
        return withAlias.append(processID(field)).toString();
    }

    /**
     * <p>Renders the JOIN portion of the query in the database-specific SQL
     * dialect.  Very few databases deviate from the standard in this matter,
     * thus the default implementation is usually sufficient.</p>
     * <p/>
     * <p>An example return value: <code>" JOIN table1 ON table.id = table1.value"</code></p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the JOIN properties.
     * @param converter The name converter to allow conversion of the query entity
     * interface into a proper table name.
     * @return The database-specific SQL rendering of the JOIN portion of the query.
     */
    protected String renderQueryJoins(Query query, TableNameConverter converter)
    {
        final StringBuilder sql = new StringBuilder();

        for (Map.Entry<Class<? extends RawEntity<?>>, String> joinEntry : query.getJoins().entrySet())
        {
            sql.append(" JOIN ").append(withSchema(converter.getName(joinEntry.getKey())));
            if (query.getAlias(joinEntry.getKey()) != null)
            {
                sql.append(" ").append(query.getAlias(joinEntry.getKey()));
            }
            if (joinEntry.getValue() != null)
            {
                sql.append(" ON ").append(processOnClause(joinEntry.getValue()));
            }
        }
        return sql.toString();
    }

    /**
     * <p>Renders the WHERE portion of the query in the database-specific SQL
     * dialect.  Very few databases deviate from the standard in this matter,
     * thus the default implementation is usually sufficient.</p>
     * <p/>
     * <p>An example return value: <code>" WHERE name = ? OR age &lt; 20"</code></p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the WHERE properties.
     * @return The database-specific SQL rendering of the WHERE portion of the query.
     */
    protected String renderQueryWhere(Query query)
    {
        StringBuilder sql = new StringBuilder();

        String whereClause = query.getWhereClause();
        if (whereClause != null)
        {
            sql.append(" WHERE ");
            sql.append(processWhereClause(whereClause));
        }

        return sql.toString();
    }

    /**
     * <p>Renders the GROUP BY portion of the query in the database-specific SQL
     * dialect.  Very few databases deviate from the standard in this matter,
     * thus the default implementation is usually sufficient.</p>
     * <p/>
     * <p>An example return value: <code>" GROUP BY name"</code></p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the GROUP BY  properties.
     * @return The database-specific SQL rendering of the GROUP BY portion of the query.
     */
    protected String renderQueryGroupBy(Query query)
    {
        StringBuilder sql = new StringBuilder();

        String groupClause = query.getGroupClause();
        if (groupClause != null)
        {
            sql.append(" GROUP BY ");
            sql.append(processGroupByClause(groupClause));
        }

        return sql.toString();
    }

    private String processGroupByClause(String groupBy)
    {
        return SqlUtils.processGroupByClause(groupBy, new Function<String, String>()
        {
            @Override
            public String apply(String field)
            {
                return processID(field);
            }
        });
    }

    /**
     * <p>Renders the ORDER BY portion of the query in the database-specific SQL
     * dialect.  Very few databases deviate from the standard in this matter,
     * thus the default implementation is usually sufficient.</p>
     * <p/>
     * <p>An example return value: <code>" ORDER BY name ASC"</code></p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the ORDER BY properties.
     * @return The database-specific SQL rendering of the ORDER BY portion of the query.
     */
    protected String renderQueryOrderBy(Query query)
    {
        StringBuilder sql = new StringBuilder();

        String orderClause = query.getOrderClause();
        if (orderClause != null)
        {
            sql.append(" ORDER BY ");
            sql.append(processOrderClause(orderClause));
        }

        return sql.toString();
    }

    private String processOrderClause(String order)
    {
        String[] orderClauses = order.split(",");
        StringBuilder sb = new StringBuilder();
        for(String orderClause : orderClauses)
        {
            // $1 signifies a RegExp matching group, i.e. group 1, to be used by search and replace.\
            // So the following will potentially quote the group definition so that the search and replace will replace the identifier with a
            // potentially quoted version of itself.
            String newClause = SqlUtils.ORDER_CLAUSE.matcher(orderClause).replaceFirst(processID("$1"));
            if(sb.length() != 0)
            {
                sb.append(",");
            }
            sb.append(newClause);
        }

        return sb.toString();
    }

    /**
     * <p>Renders the LIMIT portion of the query in the database-specific SQL
     * dialect.  There is wide variety in database implementations of this
     * particular SQL clause.  In fact, many database do not support it at all.
     * If the database in question does not support LIMIT, this method should
     * be overridden to return an empty String.  For such databases, LIMIT
     * should be implemented by overriding {@link #setQueryResultSetProperties(ResultSet, Query)}
     * and {@link #setQueryStatementProperties(Statement, Query)}.</p>
     * <p/>
     * <p>An example return value: <code>" LIMIT 10,2"</code></p>
     * <p/>
     * <p>There is usually no need to call this method directly.  Under normal
     * operations it functions as a delegate for {@link #renderQuery(Query, TableNameConverter, boolean)}.</p>
     *
     * @param query The Query instance from which to determine the LIMIT properties.
     * @return The database-specific SQL rendering of the LIMIT portion of the query.
     */
    protected String renderQueryLimit(Query query)
    {
        StringBuilder sql = new StringBuilder();

        int limit = query.getLimit();
        if (limit >= 0)
        {
            sql.append(" LIMIT ");
            sql.append(limit);
        }

        int offset = query.getOffset();
        if (offset > 0)
        {
            sql.append(" OFFSET ").append(offset);
        }

        return sql.toString();
    }

    /**
     * <p>Retrieves a JDBC {@link Connection} instance which corresponds
     * to the database represented by the provider instance. This Connection
     * can be used to execute arbitrary JDBC operations against the database.
     * Also, this is the method used by the whole of ActiveObjects itself to
     * get database connections when required.</p>
     *
     * <p>All {@link Connection} instances are pooled internally by thread.
     * Thus, there is never more than one connection per thread.  This is
     * necessary to allow arbitrary JDBC operations within a transaction
     * without breaking transaction integrity. Developers using this method
     * should bear this fact in mind and consider the {@link Connection}
     * instance immutable.  The only exception is if one is <i>absolutely</i>
     * certain that the JDBC code in question is not being executed within
     * a transaction.</p>
     *
     * <p>Despite the fact that there is only a single connection per thread,
     * the {@link Connection} instances returned from this method should
     * still be treated as bona fide JDBC connections. They can and
     * <i>should</i> be closed when their usage is complete.  This is
     * especially important when actual connection pooling is in use and
     * non-disposal of connections can lead to a crash as the connection
     * pool runs out of resources. The developer need not concern themselves
     * with the single-connection-per-thread issue when closing the connection
     * as the call to <code>close()</code> will be intercepted and ignored
     * if necessary.</p>
     * <p/>
     *
     * @return A new connection to the database
     */
    public final Connection getConnection() throws SQLException
    {
        Connection c = transactionThreadLocal.get();
        if (c != null)
        {
            if (!c.isClosed())
            {
                return c;
            }
            else
            {
                transactionThreadLocal.remove(); // remove the reference to the connection
            }
        }

        final Connection connectionImpl = dataSource.getConnection();
        if (connectionImpl == null)
        {
            throw new SQLException("Unable to create connection");
        }

        c = DelegateConnectionHandler.newInstance(connectionImpl);
        setPostConnectionProperties(c);
        return c;
    }

    public final Connection startTransaction() throws SQLException
    {
        final Connection c = getConnection();

        setCloseable(c, false);
        c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        c.setAutoCommit(false);

        transactionThreadLocal.set(c);

        return c;
    }

    public final Connection commitTransaction(Connection c) throws SQLException
    {
        checkState(c == transactionThreadLocal.get(), "There are two concurrently open transactions!");
        checkState(c != null, "Tried to commit a transaction that is not started!");

        c.commit();

        transactionThreadLocal.remove();
        return c;
    }

    public final void rollbackTransaction(Connection c) throws SQLException
    {
        checkState(c == transactionThreadLocal.get(), "There are two concurrently open transactions!");
        checkState(c != null, "Tried to rollback a transaction that is not started!");

        c.rollback();
    }

    void setCloseable(Connection connection, boolean closeable)
    {
        if (connection != null && connection instanceof DelegateConnection)
        {
            ((DelegateConnection) connection).setCloseable(closeable);
        }
    }

    /**
     * Frees any resources held by the database provider or delegate
     * libraries (such as connection pools).  This method should be
     * once usage of the provider is complete to ensure that all
     * connections are committed and closed.
     */
    public void dispose()
    {
        dataSource.dispose();
    }

    /**
     * Called to make any post-creation modifications to a new
     * {@link Connection} instance.  This is used for databases
     * such as Derby which require the schema to be set after
     * the connection is created.
     *
     * @param conn The connection to modify according to the database
     * requirements.
     */
    protected void setPostConnectionProperties(Connection conn) throws SQLException
    {
    }

    /**
     * Renders the foreign key constraints in database-specific DDL for
     * the table in question.  Actually, this method only loops through
     * the foreign keys and renders indentation and line-breaks.  The
     * actual rendering is done in a second delegate method.
     *
     * @param uniqueNameConverter
     * @param table The database-agnostic DDL representation of the table
     * in question.
     * @return The String rendering of <i>all</i> of the foreign keys for
     *         the table.
     * @see #renderForeignKey(DDLForeignKey)
     */
    protected String renderConstraintsForTable(UniqueNameConverter uniqueNameConverter, DDLTable table)
    {
        StringBuilder back = new StringBuilder();

        for (DDLForeignKey key : table.getForeignKeys())
        {
            back.append("    ").append(renderForeignKey(key)).append(",\n");
        }

        return back.toString();
    }

    /**
     * Renders the specified foreign key representation into the
     * database-specific DDL.  The implementation <i>must</i> name the
     * foreign key according to the <code>DDLForeignKey#getFKName()</code>
     * value otherwise migrations will no longer function appropriately.
     *
     * @param key The database-agnostic foreign key representation.
     * @return The database-pecific DDL fragment corresponding to the
     *         foreign key in question.
     */
    protected String renderForeignKey(DDLForeignKey key)
    {
        StringBuilder back = new StringBuilder();

        back.append("CONSTRAINT ").append(processID(key.getFKName()));
        back.append(" FOREIGN KEY (").append(processID(key.getField())).append(") REFERENCES ");
        back.append(withSchema(key.getTable())).append('(').append(processID(key.getForeignField())).append(")");

        return back.toString();
    }

    /**
     * Converts the specified type into the database-specific DDL String
     * value.  By default, this delegates to the <code>DatabaseType#getDefaultName()</code>
     * method.  Subclass implementations should be sure to make a <code>super</code>
     * call in order to ensure that both default naming and future special
     * cases are handled appropriately.
     *
     * @param type The type instance to convert to a DDL string.
     * @return The database-specific DDL representation of the type (e.g. "VARCHAR").
     */
    protected String convertTypeToString(TypeInfo<?> type)
    {
        return type.getSqlTypeIdentifier();
    }

    /**
     * Renders the specified table representation into the corresponding
     * database-specific DDL statement.  For legacy reasons, this only allows
     * single-statement table creation.  Additional statements (triggers,
     * functions, etc) must be created in one of the other delegate methods
     * for DDL creation.  This method does a great deal of delegation to
     * other <code>DatabaseProvider</code> methods for functions such as
     * field rendering, foreign key rendering, etc.
     *
     * @param nameConverters
     * @param table The database-agnostic table representation.
     * @return The database-specific DDL statements which correspond to the
     *         specified table creation.
     */
    protected final SQLAction renderTable(NameConverters nameConverters, DDLTable table)
    {
        StringBuilder back = new StringBuilder("CREATE TABLE ");
        back.append(withSchema(table.getName()));
        back.append(" (\n");

        List<String> primaryKeys = new LinkedList<String>();
        StringBuilder append = new StringBuilder();
        for (DDLField field : table.getFields())
        {
            back.append("    ").append(renderField(nameConverters, table, field, new RenderFieldOptions(true, true, true))).append(",\n");

            if (field.isPrimaryKey())
            {
                primaryKeys.add(field.getName());
            }
        }

        append.append(renderConstraintsForTable(nameConverters.getUniqueNameConverter(), table));

        back.append(append);

        if (primaryKeys.size() > 1)
        {
            throw new RuntimeException("Entities may only have one primary key");
        }

        if (primaryKeys.size() > 0)
        {
            back.append(renderPrimaryKey(table.getName(), primaryKeys.get(0)));
        }

        back.append(")");

        String tailAppend = renderAppend();
        if (tailAppend != null)
        {
            back.append(' ');
            back.append(tailAppend);
        }

        return SQLAction.of(back);
    }

    protected String renderPrimaryKey(String tableName, String pkFieldName)
    {
        StringBuilder b = new StringBuilder();
        b.append("    PRIMARY KEY(");
        b.append(processID(pkFieldName));
        b.append(")\n");
        return b.toString();
    }

    protected SQLAction renderInsert(DDLTable ddlTable, DDLValue[] ddlValues)
    {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        for (DDLValue v : ddlValues)
        {
            columns.append(processID(v.getField().getName())).append(",");
            values.append(renderValue(v.getValue())).append(",");
        }
        columns.deleteCharAt(columns.length() - 1);
        values.deleteCharAt(values.length() - 1);

        return SQLAction.of(new StringBuilder()
                .append("INSERT INTO ").append(withSchema(ddlTable.getName()))
                .append("(").append(columns).append(")")
                .append(" VALUES (").append(values).append(")"));
    }

    /**
     * Generates the appropriate database-specific DDL statement to
     * drop the specified table representation.  The default implementation
     * is merely <code>"DROP TABLE tablename"</code>.  This is suitable
     * for every database that I am aware of.  Any dependent database
     * objects (such as triggers, functions, etc) must be rendered in
     * one of the other delegate methods (such as <code>renderDropTriggers(DDLTable)</code>).
     *
     * @param table The table representation which is to be dropped.
     * @return A database-specific DDL statement which drops the specified
     *         table.
     */
    protected SQLAction renderDropTableStatement(DDLTable table)
    {
        return SQLAction.of("DROP TABLE " + withSchema(table.getName()));
    }

    /**
     * <p>Generates the database-specific DDL statements required to create
     * all of the functions, sequences, and triggers necessary for the given table,
     * by calling {@link #renderAccessoriesForField(NameConverters, DDLTable, DDLField)}
     * for each of the table's fields.  Each returned {@link SQLAction} has a
     * corresponding{@link SQLAction#getUndoAction() undo action} that deletes
     * the corresponding function, sequence, or trigger.
     *
     * @param nameConverters
     * @param table The table for which the objects must be generated.
     * @return An ordered list of {@link SQLAction}s.
     */
    protected final Iterable<SQLAction> renderAccessories(final NameConverters nameConverters, final DDLTable table)
    {
        return renderFields(
                table,
                Predicates.<DDLField>alwaysTrue(),
                new Function<DDLField, Iterable<SQLAction>>()
                {
                    @Override
                    public Iterable<SQLAction> apply(DDLField field)
                    {
                        return renderAccessoriesForField(nameConverters, table, field);
                    }
                });
    }

    /**
     * <p>Generates the database-specific DDL statements required to drop
     * all of the functions, sequences, and triggers associated with the given table,
     * by calling {@link #renderDropAccessoriesForField(NameConverters, DDLTable, DDLField)}
     * for each of the table's fields.
     *
     * @param nameConverters
     * @param table The table for which the objects must be dropped.
     * @return An ordered list of {@link SQLAction}s.
     */
    protected final Iterable<SQLAction> renderDropAccessories(final NameConverters nameConverters, final DDLTable table)
    {
        return renderFields(
                table,
                Predicates.<DDLField>alwaysTrue(),
                new Function<DDLField, Iterable<SQLAction>>()
                {
                    @Override
                    public Iterable<SQLAction> apply(DDLField field)
                    {
                        return renderDropAccessoriesForField(nameConverters, table, field);
                    }
                });
    }

    /**
     * Generates database-specific DDL statements required to create any functions,
     * sequences, or triggers required for the given field.  Each returned {@link SQLAction}
     * should have a corresponding {@link SQLAction#getUndoAction() undo action} that deletes
     * the corresponding function, sequence, or trigger.  The default implementation returns
     * an empty list.
     * @param nameConverters
     * @param table
     * @param field
     * @return an ordered list of {@link SQLAction}s
     */
    protected Iterable<SQLAction> renderAccessoriesForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return ImmutableList.of();
    }

    /**
     * Generates database-specific DDL statements required to drop any functions,
     * sequences, or triggers associated with the given field.  The default implementation
     * returns an empty list.
     * @param nameConverters
     * @param table
     * @param field
     * @return an ordered list of {@link SQLAction}s
     */
    protected Iterable<SQLAction> renderDropAccessoriesForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return ImmutableList.of();
    }
    
    protected final Iterable<SQLAction> renderFields(DDLTable table, Predicate<DDLField> filter, Function<DDLField, Iterable<SQLAction>> render)
    {
        final Iterable<DDLField> fields = Lists.newArrayList(table.getFields());
        return concat(transform(filter(fields, filter), render));
    }

    /**
     * Generates the database-specific DDL statements required to add
     * a column to an existing table.  Included in the return value
     * should be the statements required to add all necessary functions
     * and triggers to ensure that the column acts appropriately.  For
     * example, if the field is tagged with an <code>@OnUpdate</code>
     * annotation, chances are there will be a trigger and possibly a
     * function along with the ALTER statement.  These "extra"
     * functions are properly ordered and will only be appended if
     * their values are not <code>null</code>.  Because of this, very
     * few database providers will need to override this method.
     * <p>
     * Each {@link SQLAction} should have a corresponding undo action;
     * these will be executed in reverse order if the action needs to
     * be rolled back.
     *
     * @param nameConverters
     * @param table The table which should receive the new column.
     * @param field The column to add to the specified table.
     * @return An array of DDL statements to execute.
     * @see #renderFunctionForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #renderTriggerForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected Iterable<SQLAction> renderAlterTableAddColumn(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        back.add(renderAlterTableAddColumnStatement(nameConverters, table, field)
                 .withUndoAction(renderAlterTableDropColumnStatement(table, field)));

        for (DDLForeignKey foreignKey : findForeignKeysForField(table, field))
        {
            back.add(renderAlterTableAddKey(foreignKey).withUndoAction(renderAlterTableDropKey(foreignKey)));
        }

        back.addAll(renderAccessoriesForField(nameConverters, table, field));

        return back.build();
    }

    /**
     * Generates the database-specific DDL statement for adding a column,
     * but not including any corresponding sequences, triggers, etc.
     * 
     * @param nameConverters
     * @param table The table which should receive the new column.
     * @param field The column to add to the specified table.
     * @return A DDL statements to execute.
     */
    protected SQLAction renderAlterTableAddColumnStatement(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        String addStmt = "ALTER TABLE " + withSchema(table.getName()) + " ADD COLUMN " + renderField(nameConverters, table, field, new RenderFieldOptions(true, true, true));
        return SQLAction.of(addStmt);
    }
    
    /**
     * <p>Generates the database-specific DDL statements required to change
     * the given column from its old specification to the given DDL value.
     * This method will also generate the appropriate statements to remove
     * old triggers and functions, as well as add new ones according to the
     * requirements of the new field definition.</p>
     * <p/>
     * <p>The default implementation of this method functions in the manner
     * specified by the MySQL database.  Some databases will have to perform
     * more complicated actions (such as dropping and re-adding the field)
     * in order to satesfy the same use-case.  Such databases should print
     * a warning to stderr to ensure that the end-developer is aware of
     * such restrictions.</p>
     * <p/>
     * <p>Thus, the specification for this method <i>allows</i> for data
     * loss.  Nevertheless, if the database supplies a mechanism to
     * accomplish the task without data loss, it should be applied.</p>
     * <p/>
     * <p>For maximum flexibility, the default implementation of this method
     * only deals with the dropping and addition of functions and triggers.
     * The actual generation of the ALTER TABLE statement is done in the
     * {@link #renderAlterTableChangeColumnStatement(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField, net.java.ao.schema.ddl.DDLField, net.java.ao.DatabaseProvider.RenderFieldOptions)}
     * method.</p>
     *
     *
     * @param nameConverters
     * @param table The table containing the column to change.
     * @param oldField The old column definition.
     * @param field The new column definition (defining the resultant DDL).    @return An array of DDL statements to be executed.
     * @see #getTriggerNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #getFunctionNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #renderFunctionForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #renderTriggerForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected Iterable<SQLAction> renderAlterTableChangeColumn(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field)
    {
        final ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        back.addAll(renderDropAccessoriesForField(nameConverters, table, oldField));

        back.add(renderAlterTableChangeColumnStatement(nameConverters, table, oldField, field, renderFieldOptionsInAlterColumn()));

        back.addAll(renderAccessoriesForField(nameConverters, table, field));

        return back.build();
    }

    protected RenderFieldOptions renderFieldOptionsInAlterColumn()
    {
        return new RenderFieldOptions(true, true, true, true);
    }

    /**
     * Generates the database-specific DDL statement only for altering a table and
     * changing a column.  This method must only generate a single statement as it
     * does not need to concern itself with functions or triggers associated with
     * the column.  This method is only to be called as a delegate for the
     * {@link #renderAlterTableChangeColumn(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField, net.java.ao.schema.ddl.DDLField)} method,
     * for which it is a primary delegate.  The default implementation of this
     * method functions according to the MySQL specification.
     *
     *
     * @param nameConverters
     * @param table The table containing the column to change.
     * @param oldField The old column definition.
     * @param field The new column definition (defining the resultant DDL).
     * @param options
     * @return A single DDL statement which is to be executed.
     * @see #renderField(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField, net.java.ao.DatabaseProvider.RenderFieldOptions)
     */
    protected SQLAction renderAlterTableChangeColumnStatement(NameConverters nameConverters, DDLTable table, DDLField oldField, DDLField field, RenderFieldOptions options)
    {
        StringBuilder current = new StringBuilder();
        current.append("ALTER TABLE ").append(withSchema(table.getName())).append(" CHANGE COLUMN ");
        current.append(processID(oldField.getName())).append(' ');
        current.append(renderField(nameConverters, table, field, options));
        return SQLAction.of(current);
    }

    /**
     * Generates the database-specific DDL statements required to remove
     * the specified column from the given table.  This should also
     * generate the necessary statements to drop all triggers and functions
     * associated with the column in question.  If the database being
     * implemented has a non-standard syntax for dropping functions and/or
     * triggers, it may be required to override this method, even if the
     * syntax to drop columns is standard.
     *
     *
     * @param nameConverters
     * @param table The table from which to drop the column.
     * @param field The column definition to remove from the table.
     * @return An array of DDL statements to be executed.
     * @see #getTriggerNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     * @see #getFunctionNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected Iterable<SQLAction> renderAlterTableDropColumn(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        ImmutableList.Builder<SQLAction> back = ImmutableList.builder();
        
        for (DDLForeignKey foreignKey : findForeignKeysForField(table, field))
        {
            back.add(renderAlterTableDropKey(foreignKey));
        }

        back.addAll(renderDropAccessoriesForField(nameConverters, table, field));

        back.add(renderAlterTableDropColumnStatement(table, field));

        return back.build();
    }

    protected SQLAction renderAlterTableDropColumnStatement(DDLTable table, DDLField field)
    {
        String dropStmt = "ALTER TABLE " + withSchema(table.getName()) + " DROP COLUMN " + processID(field.getName());
        return SQLAction.of(dropStmt);
    }
    
    /**
     * Generates the database-specific DDL statement required to add a
     * foreign key to a table.  For databases which do not support such
     * a statement, a warning should be printed to stderr and a
     * <code>null</code> value returned.
     *
     * @param key The foreign key to be added.  As this instance contains
     * all necessary data (such as domestic table, field, etc), no
     * additional parameters are required.
     * @return A DDL statement to be executed, or <code>null</code>.
     * @see #renderForeignKey(DDLForeignKey)
     */
    protected SQLAction renderAlterTableAddKey(DDLForeignKey key)
    {
        return SQLAction.of("ALTER TABLE " + withSchema(key.getDomesticTable()) + " ADD " + renderForeignKey(key));
    }

    /**
     * Generates the database-specific DDL statement required to remove a
     * foreign key from a table.  For databases which do not support such
     * a statement, a warning should be printed to stderr and a
     * <code>null</code> value returned.  This method assumes that the
     * {@link #renderForeignKey(DDLForeignKey)} method properly names
     * the foreign key according to the {@link DDLForeignKey#getFKName()}
     * method.
     *
     * @param key The foreign key to be removed.  As this instance contains
     * all necessary data (such as domestic table, field, etc), no
     * additional parameters are required.
     * @return A DDL statement to be executed, or <code>null</code>.
     */
    protected SQLAction renderAlterTableDropKey(DDLForeignKey key)
    {
        return SQLAction.of("ALTER TABLE " + withSchema(key.getDomesticTable()) + " DROP FOREIGN KEY " + processID(key.getFKName()));
    }

    /**
     * Generates the database-specific DDL statement required to create
     * a new index.  The syntax for this operation is highly standardized
     * and thus it is unlikely this method will be overridden.  If the
     * database in question does not support indexes, a warning should
     * be printed to stderr and <code>null</code> returned.
     *
     *
     *
     * @param indexNameConverter
     * @param index The index to create.  This single instance contains all
     * of the data necessary to create the index, thus no separate
     * parameters (such as a <code>DDLTable</code>) are required.
     * @return A DDL statement to be executed.
     */
    protected SQLAction renderCreateIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        return SQLAction.of("CREATE INDEX " + withSchema(indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField())))
                                   + " ON " + withSchema(index.getTable()) + "(" + processID(index.getField()) + ")");
    }

    /**
     * Generates the database-specific DDL statement required to drop
     * an index.  The syntax for this operation is highly standardized
     * and thus it is unlikely this method will be overridden.  If the
     * database in question does not support indexes, a warning should
     * be printed to stderr and <code>null</code> returned.
     *
     *
     *
     * @param indexNameConverter
     * @param index The index to drop.  This single instance contains all
     * of the data necessary to drop the index, thus no separate
     * parameters (such as a <code>DDLTable</code>) are required.
     * @return A DDL statement to be executed, or <code>null</code>.
     */
    protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        final String indexName = indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField()));
        if (hasIndex(indexNameConverter, index))
        {
            return SQLAction.of("DROP INDEX " + withSchema(indexName) + " ON " + withSchema(index.getTable()));
        }
        else
        {
            return null;
        }
    }

    protected boolean hasIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        final String indexName = indexNameConverter.getName(shorten(index.getTable()), shorten(index.getField()));
        Connection connection = null;
        try
        {
            connection = getConnection();
            ResultSet indexes = getIndexes(connection, index.getTable());
            while (indexes.next())
            {
                if (indexName.equalsIgnoreCase(indexes.getString("INDEX_NAME")))
                {
                    return true;
                }
            }
            return false;
        }
        catch (SQLException e)
        {
            throw new ActiveObjectsException(e);
        }
        finally
        {
            closeQuietly(connection);
        }
    }

    /**
     * <p>Generates any database-specific options which must be appended
     * to the end of a table definition.  The only database I am aware
     * of which requires this is MySQL.  For example:</p>
     * <p/>
     * <pre>CREATE TABLE test (
     *     id INTEGER NOT NULL AUTO_INCREMENT,
     *     name VARCHAR(45),
     *     PRIMARY KEY(id)
     * ) ENGINE=InnoDB;</pre>
     * <p/>
     * <p>The "<code>ENGINE=InnoDB</code>" clause is what is returned by
     * this method.  The default implementation simply returns
     * <code>null</code>, signifying that no append should be rendered.</p>
     *
     * @return A DDL clause to be appended to the CREATE TABLE DDL, or <code>null</code>
     */
    protected String renderAppend()
    {
        return null;
    }

    /**
     * <p>Generates the database-specific DDL fragment required to render the
     * field and its associated type.  This includes all field attributes,
     * such as <code>@NotNull</code>, <code>@AutoIncrement</code> (if
     * supported by the database at the field level) and so on.  Sample
     * return value:</p>
     * <p/>
     * <pre>name VARCHAR(255) DEFAULT "Skye" NOT NULL</pre>
     * <p/>
     * <p>Certain databases don't allow defined precision for certain types
     * (such as Derby and INTEGER).  The logic for whether or not to render
     * precision should not be within this method, but delegated to the
     * {@link #considerPrecision(DDLField)} method.</p>
     * <p/>
     * <p>Almost all functionality within this method is delegated to other
     * methods within the implementation.  As such, it is almost never
     * necessary to override this method directly.  An exception to this
     * would be a database like PostgreSQL which requires a different type
     * for auto-incremented fields.</p>
     *
     *
     * @param nameConverters
     * @param table
     * @param field The field to be rendered.
     * @param options
     * @return A DDL fragment to be embedded in a statement elsewhere.
     */
    protected final String renderField(NameConverters nameConverters, DDLTable table, DDLField field, RenderFieldOptions options)
    {
        StringBuilder back = new StringBuilder();

        back.append(processID(field.getName()));
        back.append(" ");
        back.append(renderFieldType(field));

        if (field.isAutoIncrement())
        {
            String autoIncrementValue = renderAutoIncrement();
            if (!autoIncrementValue.trim().equals(""))
            {
                back.append(' ').append(autoIncrementValue);
            }
        }
        else if ((options.forceNull && !field.isNotNull() && !field.isUnique() && !field.isPrimaryKey()) || (options.renderDefault && field.getDefaultValue() != null))
        {
            back.append(renderFieldDefault(table, field));
        }

        if (options.renderUnique && field.isUnique())
        {
            final String renderUniqueString = renderUnique(nameConverters.getUniqueNameConverter(), table, field);

            if (!renderUniqueString.trim().equals(""))
            {
                back.append(' ').append(renderUniqueString);
            }
        }

        if (options.renderNotNull && (field.isNotNull() || field.isUnique()))
        {
            back.append(" NOT NULL");
        }

        return back.toString();
    }

    protected String renderFieldDefault(DDLTable table, DDLField field)
    {
        return new StringBuilder().append(" DEFAULT ").append(renderValue(field.getDefaultValue())).toString();
    }

    /**
     * Renders the given Java instance in a database-specific way.  This
     * method handles special cases such as {@link Calendar},
     * {@link Boolean} (which is always rendered as 0/1), functions,
     * <code>null</code> and numbers.  All other values are rendered (by
     * default) as <code>'value.toString()'</code> (the String value
     * enclosed within single quotes).  Implementations are encouraged to
     * override this method as necessary.
     *
     * @param value The Java instance to be rendered as a database literal.
     * @return The database-specific String rendering of the instance in
     *         question.
     * @see #renderDate(Date)
     */
    protected String renderValue(Object value)
    {
        if (value == null)
        {
            return "NULL";
        }
        else if (value instanceof Date)
        {
            return "'" + renderDate((Date) value) + "'";
        }
        else if (value instanceof Boolean)
        {
            return ((Boolean) value ? "1" : "0");
        }
        else if (value instanceof Number)
        {
            return value.toString();
        }

        return "'" + value.toString() + "'";
    }

    /**
     * Renders the provided {@link Date} instance as a DATETIME literal
     * in the database-specific format.  The return value should <i>not</i>
     * be enclosed within quotes, as this is accomplished within other
     * functions when rendering is required.  This method is actually a
     * boiler-plate usage of the {@link SimpleDateFormat} class, using the
     * date format defined within the {@link #getDateFormat()} method.
     *
     * @param date The time instance to be rendered.
     * @return The database-specific String representation of the time.
     */
    protected String renderDate(Date date)
    {
        return new SimpleDateFormat(getDateFormat()).format(date);
    }

    /**
     * Renders the <code>UNIQUE</code> constraint as defined by the
     * database-specific DDL syntax.  This method is a delegate of other, more
     * complex methods such as {@link #renderField(net.java.ao.schema.NameConverters, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField, net.java.ao.DatabaseProvider.RenderFieldOptions)}.  The default
     * implementation just returns <code>UNIQUE</code>.  Implementations may
     * override this method to return an empty {@link String} if the database
     * in question does not support the constraint.
     *
     * @return The database-specific rendering of <code>UNIQUE</code>.
     * @param uniqueNameConverter
     * @param table
     * @param field
     */
    protected String renderUnique(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field)
    {
        return "UNIQUE";
    }

    /**
     * Returns the database-specific TIMESTAMP text format as defined by
     * the {@link SimpleDateFormat} syntax.  This format should include
     * the time down to the second (or even more precise, if allowed by
     * the database).  The default implementation returns the format for
     * MySQL, which is: <code>yyyy-MM-dd HH:mm:ss</code>
     *
     * @return The database-specific TIMESTAMP text format
     */
    protected String getDateFormat()
    {
        return "yyyy-MM-dd HH:mm:ss";
    }

    /**
     * Renders the database-specific DDL type for the field in question.
     * This method merely delegates to the {@link #convertTypeToString(TypeInfo)}
     * method, passing the field type.  Thus, it is rarely necessary
     * (if ever) to override this method.  It may be deprecated in a
     * future release.
     *
     * @param field The field which contains the type to be rendered.
     * @return The database-specific type DDL rendering.
     */
    protected String renderFieldType(DDLField field)
    {
        return convertTypeToString(field.getType());
    }

    public Object handleBlob(ResultSet res, Class<?> type, String field) throws SQLException
    {
        final Blob blob = res.getBlob(field);
        if (type.equals(InputStream.class))
        {
            return blob.getBinaryStream();
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

    /**
     * Retrieves the name of the trigger which corresponds to the field
     * in question (if any).  If no trigger will be automatically created
     * for the specified field, <code>null</code> should be returned.
     * This function is to allow for databases which require the use of
     * triggers on a field to allow for certain functionality (like
     * ON UPDATE).  The default implementation returns <code>null</code>.
     *
     *
     * @param triggerNameConverter
     * @param table The table which contains the field for which a trigger
     * may or may not exist.
     * @param field The field for which a previous migration may have
     * created a trigger.
     * @return The unique name of the trigger which was created for the
     *         field, or <code>null</code> if none.
     * @see #renderTriggerForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.SequenceNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected String _getTriggerNameForField(TriggerNameConverter triggerNameConverter, DDLTable table, DDLField field)
    {
        return null;
    }

    /**
     * Renders the trigger which corresponds to the specified field, or
     * <code>null</code> if none.  This is to allow for databases which
     * require the use of triggers to provide functionality such as ON
     * UPDATE.  The default implementation returns <code>null</code>.
     *
     *
     *
     * @param nameConverters
     *@param table The table containing the field for which a trigger
     * may need to be rendered.
     * @param field The field for which the trigger should be rendered,
     * if any.   @return A database-specific DDL statement creating a trigger for
     *         the field in question, or <code>null</code>.
     * @see #getTriggerNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected SQLAction _renderTriggerForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return null;
    }

    /**
     * Renders SQL statement(s) to drop the trigger which corresponds to the
     * specified field, or <code>null</code> if none.
     *
     *
     * @param nameConverters
     *@param table The table containing the field for which a trigger
     * may need to be rendered.
     * @param field The field for which the trigger should be rendered,
     * if any.   @return A database-specific DDL statement to drop a trigger for
     *         the field in question, or <code>null</code>.
     */
    protected SQLAction _renderDropTriggerForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        final String trigger = _getTriggerNameForField(nameConverters.getTriggerNameConverter(), table, field);
        if (trigger != null)
        {
            return SQLAction.of("DROP TRIGGER " + processID(trigger));
        }
        return null;
    }

    /**
     * Retrieves the name of the function which corresponds to the field
     * in question (if any).  If no function will be automatically created
     * for the specified field, <code>null</code> should be returned.
     * This method is to allow for databases which require the use of
     * explicitly created functions which correspond to triggers (e.g.
     * PostgreSQL).  Few providers will need to override the default
     * implementation of this method, which returns <code>null</code>.
     *
     *
     * @param triggerNameConverter
     * @param table The table which contains the field for which a function
     * may or may not exist.
     * @param field The field for which a previous migration may have
     * created a function.
     * @return The unique name of the function which was created for the
     *         field, or <code>null</code> if none.
     */
    protected String _getFunctionNameForField(TriggerNameConverter triggerNameConverter, DDLTable table, DDLField field)
    {
        final String triggerName = _getTriggerNameForField(triggerNameConverter, table, field);
        return triggerName != null ? triggerName + "()" : null;
    }

    /**
     * Renders the function which corresponds to the specified field, or
     * <code>null</code> if none.  This is to allow for databases which
     * require the use of triggers and explicitly created functions to
     * provide functionality such as ON UPDATE (e.g. PostgreSQL).  The
     * default implementation returns <code>null</code>.
     *
     *
     *
     * @param nameConverters
     * @param table The table containing the field for which a function
     * may need to be rendered.
     * @param field The field for which the function should be rendered,
     * if any.
     * @return A database-specific DDL statement creating a function for
     *         the field in question, or <code>null</code>.
     * @see #getFunctionNameForField(net.java.ao.schema.TriggerNameConverter, net.java.ao.schema.ddl.DDLTable, net.java.ao.schema.ddl.DDLField)
     */
    protected SQLAction _renderFunctionForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return null;
    }

    /**
     * Renders SQL statement(s) to drop the function which corresponds to the
     * specified field, if applicable, or <code>null</code> otherwise.
     *
     * @param nameConverters
     * @param table The table containing the field for which a function
     * may need to be rendered.
     * @param field The field for which the function should be rendered,
     * if any.
     * @return A database-specific DDL statement to drop a function for
     *         the field in question, or <code>null</code>.
     */
    protected SQLAction _renderDropFunctionForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        final String functionName = _getFunctionNameForField(nameConverters.getTriggerNameConverter(), table, field);
        if (functionName != null)
        {
            return SQLAction.of("DROP FUNCTION " + processID(functionName));
        }
        return null;
    }

    /**
     * Renders the SQL for creating a sequence for the specified field, or
     * <code>null</code> if none.  The default implementation returns <code>null</code>.
     *
     * @param nameConverters
     * @param table The table containing the field for which a sequence
     * may need to be rendered.
     * @param field The field for which the sequence should be rendered,
     * if any.
     * @return A database-specific DDL statement creating a sequence for
     * the field in question, or <code>null</code>.
     */
    protected SQLAction _renderSequenceForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return null;
    }

    /**
     * Renders SQL statement(s) to drop the sequence which corresponds to the
     * specified field, or <code>null</code> if none.
     */
    protected SQLAction _renderDropSequenceForField(NameConverters nameConverters, DDLTable table, DDLField field)
    {
        return null;
    }
    
    /**
     * <p>Generates an INSERT statement to be used to create a new row in the
     * database, returning the primary key value.  This method also invokes
     * the delegate method, {@link #executeInsertReturningKey(EntityManager, java.sql.Connection, Class, String, String, DBParam...)}
     * passing the appropriate parameters and query.  This method is required
     * because some databases do not support the JDBC parameter
     * <code>RETURN_GENERATED_KEYS</code> (such as HSQLDB and PostgreSQL).
     * Also, some databases (such as MS SQL Server) require odd tricks to
     * support explicit value passing to auto-generated fields.  This method
     * should take care of any extra queries or odd SQL generation required
     * to implement both auto-generated primary key returning, as well as
     * explicit primary key value definition.</p>
     * <p/>
     * <p>Overriding implementations of this method should be sure to use the
     * {@link Connection} instance passed to the method, <i>not</i> a new
     * instance generated using the {@link #getConnection()} method.  This is
     * because this method is in fact a delegate invoked by {@link EntityManager}
     * as part of the entity creation process and may be part of a transaction,
     * a bulk creation or some more complicated operation.  Both optimization
     * and usage patterns on the API dictate that the specified connection
     * instance be used.  Implementations may assume that the given connection
     * instance is never <code>null</code>.</p>
     * <p/>
     * <p>The default implementation of this method should be sufficient for any
     * fully compliant ANSI SQL database with a properly implemented JDBC
     * driver.  Note that this method should <i>not</i> not actually execute
     * the SQL it generates, but pass it on to the {@link #executeInsertReturningKey(EntityManager, java.sql.Connection, Class, String, String, DBParam...)}
     * method, allowing for functional delegation and better extensibility.
     * However, this method may execute any additional statements required to
     * prepare for the INSERTion (as in the case of MS SQL Server which requires
     * some config parameters to be set on the database itself prior to INSERT).</p>
     *
     * @param manager The <code>EntityManager</code> which was used to dispatch
     * the INSERT in question.
     * @param conn The connection to be used in the eventual execution of the
     * generated SQL statement.
     * @param entityType The Java class of the entity.
     * @param pkType The Java type of the primary key value.  Can be used to
     * perform a linear search for a specified primary key value in the
     * <code>params</code> list.  The return value of the method must be of
     * the same type.
     * @param pkField The database field which is the primary key for the
     * table in question.  Can be used to perform a linear search for a
     * specified primary key value in the <code>params</code> list.
     * @param pkIdentity Flag indicating whether or not the primary key field
     * is auto-incremented by the database (IDENTITY field).
     * @param table The name of the table into which the row is to be INSERTed.
     * @param params A varargs array of parameters to be passed to the
     * INSERT statement.  This may include a specified value for the
     * primary key.
     * @throws SQLException If the INSERT fails in the delegate method, or
     * if any additional statements fail with an exception.
     * @see #executeInsertReturningKey(EntityManager, java.sql.Connection, Class, String, String, DBParam...)
     */
    @SuppressWarnings("unused")
    public <T extends RawEntity<K>, K> K insertReturningKey(EntityManager manager, Connection conn,
                                    Class<T> entityType, Class<K> pkType,
                                    String pkField, boolean pkIdentity, String table, DBParam... params) throws SQLException
    {
        final StringBuilder sql = new StringBuilder("INSERT INTO " + withSchema(table) + " (");

        for (DBParam param : params)
        {
            sql.append(processID(param.getField()));
            sql.append(',');
        }
        if (params.length > 0)
        {
            sql.setLength(sql.length() - 1);
        }
        else
        {
            sql.append(processID(pkField));
        }

        sql.append(") VALUES (");

        for (DBParam param : params)
        {
            sql.append("?,");
        }
        if (params.length > 0)
        {
            sql.setLength(sql.length() - 1);
        }
        else
        {
            sql.append("DEFAULT");
        }

        sql.append(")");

        return executeInsertReturningKey(manager, conn, entityType, pkType, pkField, sql.toString(), params);
    }

    /**
     * <p>Delegate method to execute an INSERT statement returning any auto-generated
     * primary key values.  This method is primarily designed to be called as a delegate
     * from the {@link #insertReturningKey(EntityManager, Connection, Class, String, boolean, String, DBParam...)}
     * method.  The idea behind this method is to allow custom implementations to
     * override this method to potentially execute other statements (such as getting the
     * next value in a sequence) rather than the default implementaiton which uses the
     * JDBC constant, <code>RETURN_GENERATED_KEYS</code>.  Any database which has a
     * fully-implemented JDBC driver should have no problems with the default
     * implementation of this method.</p>
     * <p/>
     * <p>Part of the design behind splitting <code>insertReturningKey</code> and
     * <code>executeInsertReturningKey</code> is so that logic for generating the actual
     * INSERT statement need not be duplicated throughout the code and in custom
     * implementations providing trivial changes to the default algorithm.  This method
     * should avoid actually generating SQL if at all possible.</p>
     * <p/>
     * <p>This method should iterate through the passed <code>DBParam(s)</code> to
     * ensure that no primary key value was explicitly specified.  If one was, it
     * should be used in leiu of one which is auto-generated by the database.  Also,
     * it is this value which should be returned if specified, rather than the value
     * which <i>would</i> have been generated or <code>null</code>.  As such, this method
     * should always return exactly the value of the primary key field in the row which
     * was just inserted, regardless of what that value may be.</p>
     * <p/>
     * <p>In cases where the database mechanism for getting the next primary key value
     * is not thread safe, this method should be declared <code>synchronized</code>,
     * or some thread synchronization technique employed.  Unfortunately, it is not
     * always possible to ensure that no other INSERT could (potentially) "steal" the
     * expected value out from under the algorithm.  Such scenarios are to be avoided
     * when possible, but the algorithm need not take extremely escoteric concurrency
     * cases into account.  (see the HSQLDB provider for an example of such a
     * less-than-thorough asynchronous algorithm)</p>
     * <p/>
     * <p><b>IMPORTANT:</b> The INSERT {@link Statement} <i>must</i> use the specified
     * connection, rather than a new one retrieved from {@link #getConnection()} or
     * equivalent.  This is because the INSERT may be part of a bulk insertion, a
     * transaction, or possibly another such operation.  It is also important to note
     * that this method should not close the connection.  Doing so could cause the
     * entity creation algorithm to fail at a higher level up the stack.</p>
     *
     * @param manager The <code>EntityManager</code> which was used to dispatch
     * the INSERT in question.
     * @param conn The database connection to use in executing the INSERT statement.
     * @param entityType The Java class of the entity.
     * @param pkType The Java class type of the primary key field (for use both in
     * searching the <code>params</code> as well as performing value conversion
     * of auto-generated DB values into proper Java instances).
     * @param pkField The database field which is the primary key for the
     * table in question.  Can be used to perform a linear search for a
     * specified primary key value in the <code>params</code> list.
     * @param params A varargs array of parameters to be passed to the
     * INSERT statement.  This may include a specified value for the
     * primary key.  @throws SQLException If the INSERT fails in the delegate method, or
     * if any additional statements fail with an exception.
     * @see #insertReturningKey(EntityManager, Connection, Class, String, boolean, String, DBParam...)
     */
    protected <T extends RawEntity<K>, K> K executeInsertReturningKey(EntityManager manager, Connection conn, 
                                              Class<T> entityType, Class<K> pkType,
                                              String pkField, String sql, DBParam... params) throws SQLException
    {
        K back = null;

        final PreparedStatement stmt = preparedStatement(conn, sql, Statement.RETURN_GENERATED_KEYS);
        
        for (int i = 0; i < params.length; i++)
        {
            Object value = params[i].getValue();

            if (value instanceof RawEntity<?>)
            {
                value = Common.getPrimaryKeyValue((RawEntity<?>) value);
            }

            if (params[i].getField().equalsIgnoreCase(pkField))
            {
                back = (K) value;
            }

            if (value == null)
            {
                putNull(stmt, i + 1);
            }
            else
            {
                TypeInfo<Object> type = (TypeInfo<Object>) typeManager.getType(value.getClass());
                type.getLogicalType().putToDatabase(manager, stmt, i + 1, value, type.getJdbcWriteType());
            }
        }

        stmt.executeUpdate();

        if (back == null)
        {
            ResultSet res = stmt.getGeneratedKeys();
            if (res.next())
            {
                back = typeManager.getType(pkType).getLogicalType().pullFromDatabase(null, res, pkType, 1);
            }
            res.close();
        }

        stmt.close();

        return back;
    }

    /**
     * Stores an SQL <code>NULL</code> value in the database.  This method
     * is required due to the fact that not all JDBC drivers handle NULLs
     * in the same fashion.  The default implementation calls {@link PreparedStatement#setNull(int, int)},
     * retrieving parameter type from metadata.  Databases which require a
     * different implementation (e.g. PostgreSQL) should override this method.
     *
     * @param stmt The statement in which to store the <code>NULL</code> value.
     * @param index The index of the parameter which should be assigned <code>NULL</code>.
     */
    public void putNull(PreparedStatement stmt, int index) throws SQLException
    {
        stmt.setNull(index, stmt.getParameterMetaData().getParameterType(index));
    }

    /**
     * Stors an SQL <code>BOOLEAN</code> value in the database.  Most databases
     * handle differences in <code>BOOLEAN</code> semantics within their JDBC
     * driver(s).  However, some do not implement the {@link PreparedStatement#setBoolean(int, boolean)}
     * method correctly.  To work around this defect, any database providers
     * for such databases should override this method to store boolean values in
     * the relevant fashion.
     *
     * @param stmt The statement in which to store the <code>BOOLEAN</code> value.
     * @param index The index of the parameter which should be assigned.
     * @param value The value to be stored in the relevant field.
     */
    public void putBoolean(PreparedStatement stmt, int index, boolean value) throws SQLException
    {
        stmt.setBoolean(index, value);
    }

    /**
     * Simple helper function used to determine of the specified JDBC
     * type is representitive of a numeric type.  The definition of
     * numeric type in this case may be assumed to be any type which
     * has a corresponding (or coercibly corresponding) Java class
     * which is a subclass of {@link Number}.  The default implementation
     * should be suitable for every conceivable use-case.
     *
     * @param type The JDBC type which is to be tested.
     * @return <code>true</code> if the specified type represents a numeric
     *         type, otherwise <code>false</code>.
     */
    protected boolean isNumericType(int type)
    {
        switch (type)
        {
            case Types.BIGINT:
                return true;
            case Types.BIT:
                return true;
            case Types.DECIMAL:
                return true;
            case Types.DOUBLE:
                return true;
            case Types.FLOAT:
                return true;
            case Types.INTEGER:
                return true;
            case Types.NUMERIC:
                return true;
            case Types.REAL:
                return true;
            case Types.SMALLINT:
                return true;
            case Types.TINYINT:
                return true;
        }

        return false;
    }

    protected String processOnClause(String on)
    {
        return SqlUtils.processOnClause(on, new Function<String, String>()
        {
            @Override
            public String apply(String id)
            {
                return processID(id);
            }
        });
    }

    /**
     * Processes the where clause to make it compatible with the given database. By default it simply escapes IDs that
     * need to be.
     *
     * @param where the raw where clause,
     * @return the processed where clause compatible with the database in use.
     * @see #processID(String)
     */
    public final String processWhereClause(String where)
    {
        return SqlUtils.processWhereClause(where, new Function<String, String>()
        {
            @Override
            public String apply(String id)
            {
                return processID(id);
            }
        });
    }

    /**
     * <p>Performs any database specific post-processing on the specified
     * identifier.  This usually means quoting reserved words, though it
     * could potentially encompass other tasks.  This method is called
     * with unbelievable frequency and thus must return extremely quickly.</p>
     * <p/>
     * <p>The default implementation checks two factors: max identifier
     * length and whether or not it represents a reserved word in the
     * underlying database.  If the identifier excedes the maximum ID
     * length for the database in question, the excess text will be
     * hashed against the hash code for the whole and concatenated with
     * the leading remainder.  The {@link #shouldQuoteID(String)} method
     * is utilitized to determine whether or not the identifier in question
     * should be quoted.  For most databases, this involves checking a set
     * of reserved words, but the method is flexible enough to allow more
     * complex "reservations rules" (such as those required by Oracle).
     * If the identifier is reserved in any way, the database-specific
     * quoting string will be retrieved from {@link DatabaseMetaData} and
     * used to enclose the identifier.  This method cannot simply quote all
     * identifiers by default due to the way that some databases (such as
     * HSQLDB and PostgreSQL) attach extra significance to quoted fields.</p>
     * <p/>
     * <p>The general assurance of this method is that for any input identfier,
     * this method will return a correspondingly-unique identifier which is
     * guarenteed to be valid within the underlying database.</p>
     *
     * @param id The identifier to process.
     * @return A unique identifier corresponding with the input which is
     *         guaranteed to function within the underlying database.
     * @see #getMaxIDLength()
     * @see #shouldQuoteID(String)
     */
    public final String processID(String id)
    {
        return quote(shorten(id));
    }

    public final String withSchema(String tableName)
    {
        final String processedTableName = processID(tableName);
        return isSchemaNotEmpty() ? schema + "." + processedTableName : processedTableName;
    }

    protected final boolean isSchemaNotEmpty()
    {
        return schema != null && schema.length() > 0;
    }

    public final String shorten(String id)
    {
        return Common.shorten(id, getMaxIDLength());
    }

    public final String quote(String id)
    {
        if (shouldQuoteID(id))
        {
            loadQuoteString();
            return quote + id + quote;
        }

        return id;
    }

    /**
     * Determines whether or not the specified identifier should be quoted
     * before transmission to the underlying database.  The default implementation
     * transforms the identifier into all-upper-case and checks the result
     * against {@link #getReservedWords()}.  Databases with more complicated
     * rules regarding quoting should provide a custom implementation of this
     * method.
     *
     * @param id The identifier to check against the quoting rules.
     * @return <code>true</code> if the specified identifier is invalid under
     *         the relevant quoting rules, otherwise <code>false</code>.
     */
    protected boolean shouldQuoteID(String id)
    {
        return getReservedWords().contains(Case.UPPER.apply(id));
    }

    /**
     * Returns the maximum length for any identifier in the underlying database.
     * If the database defines different maximum lengths for different identifier
     * types, the <i>minimum</i> value should be returned by this method.  By
     * default, this just returns {@link Integer#MAX_VALUE}.
     *
     * @return The maximum identifier length for the database.
     */
    protected int getMaxIDLength()
    {
        return Integer.MAX_VALUE;
    }

    /**
     * Retrieves the set of all reserved words for the underlying database.  The
     * set returns should be speculative, meaning that it should include any
     * <i>possible</i> reserved words, not just those for a particular version.
     * As an implementation guideline, the {@link Set} instance returned from this
     * method should guarentee <i>O(1)</i> lookup times, otherwise ORM performance
     * will suffer greatly.
     *
     * @return A set of <i>upper case</i> reserved words specific
     *         to the database.
     */
    protected abstract Set<String> getReservedWords();

    /**
     * Flag indicating whether or not the underlying database uses case-sensitive
     * identifiers.  This specifically affects comparisons in the {@link SchemaReader}
     * utility.  The default value is <code>true</code>.  Note that databases which
     * support both case-sensetive and case-insensetive identifiers (like MySQL) should
     * return <code>true</code> for better all-around compatibility.
     *
     * @return boolean <code>true</code> if identifiers are case-sensetive,
     *         <code>false</code> otherwise.
     */
    public boolean isCaseSensitive()
    {
        return true;
    }

    /**
     * Tells whether this exception should be ignored when running an updated statement. Typically, errors on dropping
     * non-existing objects should be ignored.
     *
     * @param sql
     * @param e the {@link java.sql.SQLException} that occured.
     * @throws SQLException throws the SQLException if it should not be ignored.
     */
    public void handleUpdateError(String sql, SQLException e) throws SQLException
    {
        sqlLogger.error("Exception executing SQL update <" + sql + ">", e);
        throw e;
    }

    public final PreparedStatement preparedStatement(Connection c, CharSequence sql) throws SQLException
    {
        final String sqlString = sql.toString();
        onSql(sqlString);
        return c.prepareStatement(sqlString);
    }

    public final PreparedStatement preparedStatement(Connection c, CharSequence sql, int autoGeneratedKeys) throws SQLException
    {
        final String sqlString = sql.toString();
        onSql(sqlString);
        return c.prepareStatement(sqlString, autoGeneratedKeys);
    }

    public final PreparedStatement preparedStatement(Connection c, CharSequence sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        final String sqlString = sql.toString();
        onSql(sqlString);
        return c.prepareStatement(sqlString, resultSetType, resultSetConcurrency);
    }

    public final void executeUpdate(Statement stmt, CharSequence sql) throws SQLException
    {
        final String sqlString = sql.toString();
        try
        {
            onSql(sqlString);
            checkNotNull(stmt).executeUpdate(sqlString);
        }
        catch (SQLException e)
        {
            handleUpdateError(sqlString, e);
        }
    }

    /**
     * Attempt to execute a list of actions that make up a logical unit (e.g. adding an entire
     * table, or adding a new column to an existing table).  If any action fails, throw an
     * SQLException, but first go backward through all successfully completed actions in this
     * list and execute their corresponding undo action, if any.  For instance, if we successfully
     * executed a CREATE TABLE and a CREATE SEQUENCE, but the next statement fails, we will
     * execute DROP SEQUENCE and then DROP TABLE before rethrowing the exception.
     * 
     * @param provider
     * @param stmt  A JDBC Statement that will be reused for all updates
     * @param actions  A list of {@link SQLAction}s to execute
     * @param completedStatements  A set of SQL statements that should not be executed if we encounter the same one again.
     *   This is necessary because our schema diff logic is not as smart as it could be, so it may
     *   tell us, for instance, to create an index for a new column even though the statements for
     *   creating the column also included creation of the index.
     * @return all SQL statements that were executed
     */
    public final Iterable<String> executeUpdatesForActions(Statement stmt, Iterable<SQLAction> actions, Set<String> completedStatements) throws SQLException
    {
        Stack<SQLAction> completedActions = new Stack<SQLAction>();
        Set<String> newStatements = new LinkedHashSet<String>();
        for (SQLAction action : actions)
        {
            try
            {
                addAll(newStatements, executeUpdateForAction(stmt, action, union(completedStatements, newStatements)));
            }
            catch (SQLException e)
            {
                logger.warn("Error in schema creation: " + e.getMessage() + "; attempting to roll back last partially generated table");
                while (!completedActions.isEmpty())
                {
                    SQLAction undoAction = completedActions.pop().getUndoAction();
                    if (undoAction != null)
                    {
                        try
                        {
                            executeUpdateForAction(stmt, undoAction, completedStatements);
                        }
                        catch (SQLException e2)
                        {
                            logger.warn("Unable to finish rolling back partial table creation due to error: " + e2.getMessage());
                            // swallow this exception because we're going to rethrow the original exception
                            break;
                        }
                    }
                }
                // rethrow the original exception
                throw e;
            }
            completedActions.push(action);
        }
        return newStatements;
    }
    
    public final Iterable<String> executeUpdateForAction(Statement stmt, SQLAction action, Set<String> completedStatements) throws SQLException
    {
        String sql = action.getStatement().trim();
        if (sql.isEmpty() || completedStatements.contains(sql))
        {
            return ImmutableList.of();
        }
        executeUpdate(stmt, sql);
        return ImmutableList.of(sql);
    }

    public final void addSqlListener(SqlListener l)
    {
        sqlListeners.add(l);
    }

    public final void removeSqlListener(SqlListener l)
    {
        sqlListeners.remove(l);
    }

    protected final void onSql(String sql)
    {
        for (SqlListener sqlListener : sqlListeners)
        {
            sqlListener.onSql(sql);
        }
    }

    private static boolean isBlank(String str)
    {
        int strLen;
        if (str == null || (strLen = str.length()) == 0)
        {
            return true;
        }
        for (int i = 0; i < strLen; i++)
        {
            if (!Character.isWhitespace(str.charAt(i)))
            {
                return false;
            }
        }
        return true;
    }

    protected Iterable<DDLForeignKey> findForeignKeysForField(DDLTable table, final DDLField field)
    {
        return Iterables.filter(newArrayList(table.getForeignKeys()), new Predicate<DDLForeignKey>()
        {
            @Override
            public boolean apply(DDLForeignKey fk)
            {
                return fk.getField().equals(field.getName());
            }
        });
    }

    protected static class RenderFieldOptions
    {
        public final boolean renderUnique;
        public final boolean renderDefault;
        public final boolean renderNotNull;
        public final boolean forceNull;

        public RenderFieldOptions(boolean renderUnique, boolean renderDefault, boolean renderNotNull)
        {
            this(renderUnique, renderDefault, renderNotNull, false);
        }

        public RenderFieldOptions(boolean renderUnique, boolean renderDefault, boolean renderNotNull, boolean forceNull)
        {
            this.renderUnique = renderUnique;
            this.renderDefault = renderDefault;
            this.renderNotNull = renderNotNull;
            this.forceNull = forceNull;
        }
    }

    public static interface SqlListener
    {
        void onSql(String sql);
    }

    private final static class LoggingSqlListener implements SqlListener
    {
        private final Logger logger;

        public LoggingSqlListener(Logger logger)
        {
            this.logger = checkNotNull(logger);
        }

        public void onSql(String sql)
        {
            logger.debug(sql);
        }
    }
}