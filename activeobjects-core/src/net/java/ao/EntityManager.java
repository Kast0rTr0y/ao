/*
 * Copyright 2007 Daniel Spiewak
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import net.java.ao.schema.CachingNameConverters;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.info.EntityInfo;
import net.java.ao.schema.info.FieldInfo;
import net.java.ao.schema.info.EntityInfoResolver;
import net.java.ao.types.TypeInfo;
import net.java.ao.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.getValueFieldsNames;
import static net.java.ao.Common.preloadValue;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static org.apache.commons.lang.ArrayUtils.contains;

/**
 * <p>The root control class for the entire ActiveObjects API.  <code>EntityManager</code> is the source of all {@link
 * RawEntity} objects, as well as the dispatch layer between the entities, the pluggable table name converters, and the
 * database abstraction layers.  This is the entry point for any use of the API.</p>
 *
 * <p><code>EntityManager</code> is designed to be used in an instance fashion with each instance corresponding to a
 * single database.  Thus, rather than a singleton instance or a static factory method, <code>EntityManager</code> does
 * have a proper constructor.  Any static instance management is left up to the developer using the API.</p>
 *
 * @author Daniel Spiewak
 */
public class EntityManager
{

    private static final Logger log = LoggerFactory.getLogger(EntityManager.class);

    private final DatabaseProvider provider;
    private final EntityManagerConfiguration configuration;

    private final SchemaConfiguration schemaConfiguration;
    private final NameConverters nameConverters;

    private final EntityInfoResolver entityInfoResolver;

    private PolymorphicTypeMapper typeMapper;
    private final ReadWriteLock typeMapperLock = new ReentrantReadWriteLock(true);

    private final com.google.common.cache.Cache<Class<? extends ValueGenerator<?>>, ValueGenerator<?>> valGenCache;

    /**
     * Creates a new instance of <code>EntityManager</code> using the specified {@link DatabaseProvider}.
     *
     * @param provider the {@link DatabaseProvider} to use in all database operations.
     * @param configuration the configuration for this entity manager
     */
    public EntityManager(DatabaseProvider provider, EntityManagerConfiguration configuration)
    {
        this.provider = checkNotNull(provider);
        this.configuration = checkNotNull(configuration);
        valGenCache = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends ValueGenerator<?>>, ValueGenerator<?>>() {
            @Override
            public ValueGenerator<?> load(Class<? extends ValueGenerator<?>> generatorClass) throws Exception {
                return generatorClass.newInstance();
            }
        });

        // TODO: move caching out of there!
        nameConverters = new CachingNameConverters(configuration.getNameConverters());
        schemaConfiguration = checkNotNull(configuration.getSchemaConfiguration());

        typeMapper = new DefaultPolymorphicTypeMapper(new HashMap<Class<? extends RawEntity<?>>, String>());

        entityInfoResolver = checkNotNull(configuration.getEntityInfoResolverFactory().create(nameConverters, provider.getTypeManager()), "entityInfoResolver");
    }


    /**
     * Convenience method to create the schema for the specified entities using the current settings (table/field name
     * converter and database provider).
     *
     * @param entities the "list" of entity classes to consider for migration.
     * @see SchemaGenerator#migrate(DatabaseProvider, SchemaConfiguration, NameConverters, boolean, Class[])
     */
    public void migrate(Class<? extends RawEntity<?>>... entities) throws SQLException
    {
        SchemaGenerator.migrate(provider, schemaConfiguration, nameConverters, false, entities);
    }

    /**
     * Convenience method to create the schema for the specified entities using the current settings (table/field name
     * converter and database provider). Note that if the given entities do not include the full set of entities, or
     * those entities have removed any fields, then the corresponding tables or columns will be dropped, and <b>any data
     * they contained will be lost</b>. Use this at your own risk.
     *
     * @param entities the "list" of entity classes to consider for migration.
     * @see SchemaGenerator#migrate(DatabaseProvider, SchemaConfiguration, NameConverters, boolean, Class[])
     */
    public void migrateDestructively(Class<? extends RawEntity<?>>... entities) throws SQLException
    {
        SchemaGenerator.migrate(provider, schemaConfiguration, nameConverters, true, entities);
    }

    /**
     * @deprecated since 0.23. EntityManager now no longer caches entities.
     * use {@link #flush(RawEntity[])} to flush values for individual entities
     */
    @Deprecated
    public void flushAll()
    {
        // no-op
    }

    /**
     * @deprecated since 0.23. EntityManager now no longer caches entities.
     * use {@link #flush(RawEntity[])} to flush values for individual entities
     */
    @Deprecated
    public void flushEntityCache()
    {
        // no-op
    }

    /**
     * @deprecated since 0.25. Entities and values now no longer cached.
     */
    @Deprecated
	public void flush(RawEntity<?>... entities) {
        // no-op
	}

	/**
	 * <p>Returns an array of entities of the specified type corresponding to the
	 * varargs primary keys.  If an in-memory reference already exists to a corresponding
	 * entity (of the specified type and key), it is returned rather than creating
	 * a new instance.</p>
	 *
	 * <p>If the entity is known to exist in the database, then no checks are performed
	 * and the method returns extremely quickly.  However, for any key which has not
	 * already been verified, a query to the database is performed to determine whether
	 * or not the entity exists.  If the entity does not exist, then <code>null</code>
	 * is returned.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param keys	The primary keys corresponding to the entities to retrieve.  All
	 * 	keys must be typed according to the generic type parameter of the entity's
	 * 	{@link RawEntity} inheritence (if inheriting from {@link Entity}, this is <code>Integer</code>
	 * 	or <code>int</code>).  Thus, the <code>keys</code> array is type-checked at compile
	 * 	time.
	 * @return An array of entities of the given type corresponding with the specified
	 * 		primary keys.  Any entities which are non-existent will correspond to a <code>null</code>
	 * 		value in the resulting array.
	 */
	public <T extends RawEntity<K>, K> T[] get(final Class<T> type, K... keys) throws SQLException
    {
        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);
        final String primaryKeyField = entityInfo.getPrimaryKey().getName();
        return get(type, findByPrimaryKey(type, primaryKeyField), keys);
    }

    private <T extends RawEntity<K>, K> Function<T, K> findByPrimaryKey(final Class<T> type, final String primaryKeyField)
    {
        return new Function<T, K>()
        {
            @Override
            public T invoke(K k) throws SQLException
            {
                final T[] ts = find(type, primaryKeyField + " = ?", k);
                if (ts.length == 1)
                {
                    return ts[0];
                }
                else if (ts.length == 0)
                {
                    return null;
                }
                else
                {
                    throw new ActiveObjectsException("Found more that one object of type '" + type.getName() + "' for key '" + k + "'");
                }
            }
        };
    }

    protected <T extends RawEntity<K>, K> T[] peer(final EntityInfo<T, K> entityInfo, K... keys) throws SQLException
    {
        return get(entityInfo.getEntityType(), new Function<T, K>()
        {
            public T invoke(K key)
            {
                return getAndInstantiate(entityInfo, key);
            }
        }, keys);
    }


    private <T extends RawEntity<K>, K> T[] get(Class<T> type, Function<T, K> create, K... keys) throws SQLException
    {
        //noinspection unchecked
        T[] back = (T[])Array.newInstance(type, keys.length);
        int index = 0;

		for (K key : keys) {
            back[index++] = create.invoke(key);
        }

        return back;
    }

    /**
     * Creates a new instance of the entity of the specified type corresponding to the given primary key.  This is used
     * by {@link #get(Class, Object[])}} to create the entity.
     *
     * @param entityInfo The type of the entity to create.
     * @param key The primary key corresponding to the entity instance required.
     * @return An entity instance of the specified type and primary key.
     */
    protected <T extends RawEntity<K>, K> T getAndInstantiate(EntityInfo<T, K> entityInfo, K key)
    {
        Class<T> type = entityInfo.getEntityType();
        EntityProxy<T, K> proxy = new EntityProxy<T, K>(this, entityInfo, key);

		T entity = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class[]{type, EntityProxyAccessor.class}, proxy));
		return entity;
	}

    /**
     * Cleverly overloaded method to return a single entity of the specified type rather than an array in the case where
     * only one ID is passed.  This method meerly delegates the call to the overloaded <code>get</code> method and
     * functions as syntactical sugar.
     *
     * @param type The type of the entity instance to retrieve.
     * @param key The primary key corresponding to the entity to be retrieved.
     * @return An entity instance of the given type corresponding to the specified primary key, or <code>null</code> if
     *         the entity does not exist in the database.
     * @see #get(Class, Object[])
     */
    public <T extends RawEntity<K>, K> T get(Class<T> type, K key) throws SQLException
    {
        return get(type, toArray(key))[0];
    }

    protected <T extends RawEntity<K>, K> T peer(EntityInfo<T, K> entityInfo, K key) throws SQLException
    {
        return peer(entityInfo, toArray(key))[0];
    }

    @SuppressWarnings("unchecked")
    private static <K> K[] toArray(K key)
    {
        return (K[])new Object[]{key};
    }

    /**
	 * <p>Creates a new entity of the specified type with the optionally specified
	 * initial parameters.  This method actually inserts a row into the table represented
	 * by the entity type and returns the entity instance which corresponds to that
	 * row.</p>
	 *
	 * <p>The {@link DBParam} object parameters are designed to allow the creation
	 * of entities which have non-null fields which have no defalut or auto-generated
	 * value.  Insertion of a row without such field values would of course fail,
	 * thus the need for db params.  The db params can also be used to set
	 * the values for any field in the row, leading to more compact code under
	 * certain circumstances.</p>
	 *
	 * <p>Unless within a transaction, this method will commit to the database
	 * immediately and exactly once per call.  Thus, care should be taken in
	 * the creation of large numbers of entities.  There doesn't seem to be a more
	 * efficient way to create large numbers of entities, however one should still
	 * be aware of the performance implications.</p>
	 *
	 * <p>This method delegates the action INSERT action to
	 * {@link DatabaseProvider#insertReturningKey}.
	 * This is necessary because not all databases support the JDBC <code>RETURN_GENERATED_KEYS</code>
	 * constant (e.g. PostgreSQL and HSQLDB).  Thus, the database provider itself is
	 * responsible for handling INSERTion and retrieval of the correct primary key
	 * value.</p>
	 *
	 * @param type		The type of the entity to INSERT.
	 * @param params	An optional varargs array of initial values for the fields in the row.  These
	 * 	values will be passed to the database within the INSERT statement.
	 * @return	The new entity instance corresponding to the INSERTed row.
	 * @see net.java.ao.DBParam
	 */
	public <T extends RawEntity<K>, K> T create(Class<T> type, DBParam... params) throws SQLException {
		T back = null;
        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);
		final String table = entityInfo.getName();

        Set<DBParam> listParams = new HashSet<DBParam>();
        listParams.addAll(Arrays.asList(params));

        for (FieldInfo fieldInfo : Iterables.filter(entityInfo.getFields(), FieldInfo.HAS_GENERATOR))
        {

            ValueGenerator<?> generator;
            try
            {
                generator = valGenCache.get(fieldInfo.getGeneratorType());
            }
            catch (ExecutionException e)
            {
                throw Throwables.propagate(e.getCause());
            }
            catch (UncheckedExecutionException e)
            {
                throw Throwables.propagate(e.getCause());
            }
            catch (ExecutionError e)
            {
                throw Throwables.propagate(e.getCause());
            }

            listParams.add(new DBParam(fieldInfo.getName(), generator.generateValue(this)));
        }

        Set<FieldInfo> requiredFields = Sets.newHashSet(Sets.filter(entityInfo.getFields(), FieldInfo.IS_REQUIRED));

        for (DBParam param : listParams)
        {
            FieldInfo field = entityInfo.getField(param.getField());
            checkNotNull(field, "Entity %s does not have field %s", type.getName(), param.getField());

            if (field.isPrimary())
            {
                //noinspection unchecked
                Common.validatePrimaryKey(field, param.getValue());
            }
            else if (!field.isNullable())
            {
                checkArgument(param.getValue() != null, "Cannot set non-null field %s to null", param.getField());
                if (param.getValue() instanceof String)
                {
                    checkArgument(!StringUtils.isBlank((String) param.getValue()), "Cannot set non-null String field %s to ''", param.getField());
                }
            }

            requiredFields.remove(field);

            final TypeInfo dbType = field.getTypeInfo();
            if (dbType != null && param.getValue() != null)
            {
                dbType.getLogicalType().validate(param.getValue());
            }
        }

        if (!requiredFields.isEmpty())
        {
            throw new IllegalArgumentException("The follow required fields were not set when trying to create entity '" + type.getName() + "', those fields are: " + Joiner.on(", ").join(requiredFields));
        }

        Connection connection = null;
        try
        {
            connection = provider.getConnection();
            back = peer(entityInfo, provider.insertReturningKey(this, connection,
                                                          type,
                                                          entityInfo.getPrimaryKey().getJavaType(),
                                                          entityInfo.getPrimaryKey().getName(),
                                                          entityInfo.getPrimaryKey().hasAutoIncrement(),
                                                          table, listParams.toArray(new DBParam[listParams.size()])));
        }
        finally
        {
            closeQuietly(connection);
        }
		back.init();
		return back;
	}

    /**
     * Creates and INSERTs a new entity of the specified type with the given map of parameters.  This method merely
     * delegates to the {@link #create(Class, DBParam...)} method.  The idea behind having a separate convenience method
     * taking a map is in circumstances with large numbers of parameters or for people familiar with the anonymous inner
     * class constructor syntax who might be more comfortable with creating a map than with passing a number of
     * objects.
     *
     * @param type The type of the entity to INSERT.
     * @param params A map of parameters to pass to the INSERT.
     * @return The new entity instance corresponding to the INSERTed row.
     * @see #create(Class, DBParam...)
     */
    public <T extends RawEntity<K>, K> T create(Class<T> type, Map<String, Object> params) throws SQLException
    {
        DBParam[] arrParams = new DBParam[params.size()];
        int i = 0;

        for (String key : params.keySet())
        {
            arrParams[i++] = new DBParam(key, params.get(key));
        }

        return create(type, arrParams);
    }

    /**
     * <p>Deletes the specified entities from the database.  DELETE statements are called on the rows in the
     * corresponding tables.  The entity instances themselves are not invalidated, but it doesn't even make sense to
     * continue using the instance without a row with which it is paired.</p>
     *
     * <p>This method does attempt to group the DELETE statements on a per-type basis.  Thus, if you pass 5 instances of
     * <code>EntityA</code> and two instances of <code>EntityB</code>, the following SQL prepared statements will be
     * invoked:</p>
     *
     * <pre>DELETE FROM entityA WHERE id IN (?,?,?,?,?);
     * DELETE FROM entityB WHERE id IN (?,?);</pre>
     *
     * <p>Thus, this method scales very well for large numbers of entities grouped into types.  However, the execution
     * time increases linearly for each entity of unique type.</p>
     *
     * @param entities A varargs array of entities to delete.  Method returns immediately if length == 0.
     */
    @SuppressWarnings("unchecked")
    public void delete(RawEntity<?>... entities) throws SQLException
    {
        if (entities.length == 0)
        {
            return;
        }

        Map<Class<? extends RawEntity<?>>, List<RawEntity<?>>> organizedEntities =
            new HashMap<Class<? extends RawEntity<?>>, List<RawEntity<?>>>();

        for (RawEntity<?> entity : entities)
        {
            verify(entity);
            Class<? extends RawEntity<?>> type = getProxyForEntity(entity).getType();

            if (!organizedEntities.containsKey(type))
            {
                organizedEntities.put(type, new LinkedList<RawEntity<?>>());
            }
            organizedEntities.get(type).add(entity);
        }

        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = provider.getConnection();
            for (Class type : organizedEntities.keySet()) {
                EntityInfo entityInfo = resolveEntityInfo(type);
                List<RawEntity<?>> entityList = organizedEntities.get(type);

                StringBuilder sql = new StringBuilder("DELETE FROM ");
                sql.append(provider.withSchema(entityInfo.getName()));
                sql.append(" WHERE ").append(provider.processID(entityInfo.getPrimaryKey().getName())).append(" IN (?");

                for (int i = 1; i < entityList.size(); i++) {
                    sql.append(",?");
                }
                sql.append(')');

                stmt = provider.preparedStatement(conn, sql);

                int index = 1;
                for (RawEntity<?> entity : entityList) {
                    TypeInfo typeInfo = entityInfo.getPrimaryKey().getTypeInfo();
                    typeInfo.getLogicalType().putToDatabase(this, stmt, index++, Common.getPrimaryKeyValue(entity), typeInfo.getJdbcWriteType());
                }
                stmt.executeUpdate();
            }
        }
        finally
        {
            closeQuietly(stmt);
            closeQuietly(conn);
        }
    }

    /**
     * <p>Deletes rows from the table corresponding to {@code type}. In contrast to {@link #delete(RawEntity[])}, this
     * method allows you to delete rows without creating entities for them first.</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.deleteWithSQL(Person.class, "name = ?", "Charlie")</pre>
     *
     * <p>The SQL in {@code criteria} is not parsed or modified in any way by ActiveObjects, and is simply appended to
     * the DELETE statement in a WHERE clause. The above example would cause an SQL statement similar to the following
     * to be executed:</p>
     *
     * <pre>DELETE FROM people WHERE name = 'Charlie';</pre>
     *
     * <p>If {@code criteria} is {@code null}, this method deletes all rows from the table corresponding to {@code
     * type}.</p>
     *
     * <p>This method does not attempt to determine the set of entities affected by the statement. As such, it is
     * recommended that you call {@link #flushAll()} after calling this method.</p>
     *
     * @param type The entity type corresponding to the table to delete from.
     * @param criteria An optional SQL fragment specifying which rows to delete.
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement. The length of
     * this array <i>must</i> match the number of parameters (denoted by the '?' char) in {@code criteria}.
     * @return The number of rows deleted from the table.
     * @see #delete(RawEntity...)
     * @see #find(Class, String, Object...)
     * @see #findWithSQL(Class, String, String, Object...)
     */
    public <K> int deleteWithSQL(Class<? extends RawEntity<K>> type, String criteria, Object... parameters) throws SQLException
    {
        int rowCount = 0;
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(provider.withSchema(nameConverters.getTableNameConverter().getName(type)));

        if (criteria != null)
        {
            sql.append(" WHERE ");
            sql.append(provider.processWhereClause(criteria));
        }

        final Connection connection = provider.getConnection();
        try
        {
            final PreparedStatement stmt = provider.preparedStatement(connection, sql);
            try
            {
                putStatementParameters(stmt, parameters);
                rowCount = stmt.executeUpdate();
            }
            finally
            {
                stmt.close();
            }
        }
        finally
        {
            connection.close();
        }

        return rowCount;
    }

    /**
     * Returns all entities of the given type.  This actually peers the call to the {@link #find(Class, Query)} method.
     *
     * @param type The type of entity to retrieve.
     * @return An array of all entities which correspond to the given type.
     */
    public <T extends RawEntity<K>, K> T[] find(Class<T> type) throws SQLException
    {
        return find(type, Query.select());
    }

    /**
     * <p>Convenience method to select all entities of the given type with the specified, parameterized criteria.  The
     * <code>criteria</code> String specified is appended to the SQL prepared statement immediately following the
     * <code>WHERE</code>.</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.find(Person.class, "name LIKE ? OR age &gt; ?", "Joe", 9);</pre>
     *
     * <p>This actually delegates the call to the {@link #find(Class, Query)} method, properly parameterizing the {@link
     * Query} object.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param criteria A parameterized WHERE statement used to determine the results.
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement.  The length of
     * this array <i>must</i> match the number of parameters (denoted by the '?' char) in the <code>criteria</code>.
     * @return An array of entities of the given type which match the specified criteria.
     */
    public <T extends RawEntity<K>, K> T[] find(Class<T> type, String criteria, Object... parameters) throws SQLException
    {
        return find(type, Query.select().where(criteria, parameters));
    }

    /**
     * <p>Convenience method to select a single entity of the given type with the specified, parameterized criteria. The
     * <code>criteria</code> String specified is appended to the SQL prepared statement immediately following the
     * <code>WHERE</code>.</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.findSingleEntity(Person.class, "name LIKE ? OR age &gt; ?", "Joe", 9);</pre>
     *
     * <p>This actually delegates the call to the {@link #find(Class, String, Object...)} method, properly
     * parameterizing the {@link Object} object.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param criteria A parameterized WHERE statement used to determine the results.
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement.  The length of
     * this array <i>must</i> match the number of parameters (denoted by the '?' char) in the <code>criteria</code>.
     * @return A single entity of the given type which match the specified criteria or null if none returned
     */
    public <T extends RawEntity<K>, K> T findSingleEntity(Class<T> type, String criteria, Object... parameters) throws SQLException
    {
        T[] entities = find(type, criteria, parameters);

        if (entities.length < 1)
        {
            return null;
        }
        else if (entities.length > 1)
        {
            throw new IllegalStateException("Found more than one entities of type '"
                                                + type.getSimpleName() + "' that matched the criteria '" + criteria
                                                + "' and parameters '" + parameters.toString() + "'.");
        }

        return entities[0];
    }

    /**
     * <p>Selects all entities matching the given type and {@link Query}.  By default, the entities will be created
     * based on the values within the primary key field for the specified type (this is usually the desired
     * behavior).</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.find(Person.class, Query.select().where("name LIKE ? OR age &gt; ?", "Joe", 9).limit(10));</pre>
     *
     * <p>This method delegates the call to {@link #find(Class, String, Query)}, passing the primary key field for the
     * given type as the <code>String</code> parameter.</p>
     *
     * <p>Note that in the case of calling this function with a {@link net.java.ao.Query} with select fields, the
     * first field will be passed to {@link #find(Class, String, Query)}. If this is not the intention, a direct
     * call to {@link #find(Class, String, Query)} should be made instead, with the primary key field specified
     * and present in the select fields.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param query The {@link Query} instance to be used to determine the results.
     * @return An array of entities of the given type which match the specified query.
     */
    public <T extends RawEntity<K>, K> T[] find(Class<T> type, Query query) throws SQLException
    {
        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);

        query.resolvePrimaryKey(entityInfo.getPrimaryKey());
        String selectField = entityInfo.getPrimaryKey().getName();

        Iterable<String> fields = query.getFields();
        if (Iterables.size(fields) == 1)
        {
            selectField = Iterables.get(fields, 0);
        }

        return find(type, selectField, query);
    }

    /**
     * <p>Selects all entities of the specified type which match the given <code>Query</code>.  This method creates a
     * <code>PreparedStatement</code> using the <code>Query</code> instance specified against the table represented by
     * the given type.  This query is then executed (with the parameters specified in the query).  The method then
     * iterates through the result set and extracts the specified field, mapping an entity of the given type to each
     * row.  This array of entities is returned.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param field The field value to use in the creation of the entities.  This is usually the primary key field of
     * the corresponding table.
     * @param query The {@link Query} instance to use in determining the results.
     * @return An array of entities of the given type which match the specified query.
     */
    public <T extends RawEntity<K>, K> T[] find(Class<T> type, String field, Query query) throws SQLException
    {
        List<T> back = new ArrayList<T>();


        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);

        query.resolvePrimaryKey(entityInfo.getPrimaryKey());

        // legacy support for "*" in the select - to be removed after AO-552 implemented
        boolean starSelected = false;
        for (final String selectedField : query.getFields())
        {
            if ("*".equals(selectedField))
            {
                // change the field to the PK; not sure how this ever worked, but being safe
                field = entityInfo.getPrimaryKey().getName();

                starSelected = true;
                break;
            }
        }

        final Preload preloadAnnotation = type.getAnnotation(Preload.class);
        final Set<String> selectedFields;
        if (starSelected || preloadAnnotation == null || contains(preloadAnnotation.value(), Preload.ALL))
        {
            // select all fields from the table - no preload is specified, the user has asked for all or "*" is selected
            selectedFields = getValueFieldsNames(entityInfo, nameConverters.getFieldNameConverter());
        }
        else
        {
            // select user's selection, as well as any specific preloads
            selectedFields = new HashSet<String>(preloadValue(preloadAnnotation, nameConverters.getFieldNameConverter()));
            for (String existingField : query.getFields())
            {
                selectedFields.add(existingField);
            }
        }
        query.setFields(selectedFields.toArray(new String[selectedFields.size()]));

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            conn = provider.getConnection();
            final String sql = query.toSQL(entityInfo, provider, getTableNameConverter(), false);

            stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            provider.setQueryStatementProperties(stmt, query);

            query.setParameters(this, stmt);

            res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);

            final TypeInfo<K> primaryKeyType = entityInfo.getPrimaryKey().getTypeInfo();
            final Class<K> primaryKeyClassType = entityInfo.getPrimaryKey().getJavaType();
            final String[] canonicalFields = query.getCanonicalFields(entityInfo);
            while (res.next())
            {
                final T entity = peer(entityInfo, primaryKeyType.getLogicalType().pullFromDatabase(this, res, primaryKeyClassType, field));
                final Map<String, Object> values = new HashMap<String, Object>();
                for (String name : canonicalFields)
                {
                    TypeInfo<K> fieldInfo = entityInfo.getField(name).getTypeInfo();
                    values.put(name, fieldInfo.getLogicalType().pullFromDatabase(this,res,entityInfo.getField(name).getJavaType(), name));
                }
                if (!values.isEmpty())
                {
                    final EntityProxy<?, ?> proxy = getProxyForEntity(entity);
                    proxy.updateValues(values);
                }
                back.add(entity);
            }
        }
        finally
        {
            closeQuietly(res, stmt, conn);
        }
        return back.toArray((T[]) Array.newInstance(type, back.size()));
    }

    /**
     * <p>Executes the specified SQL and extracts the given key field, wrapping each row into a instance of the
     * specified type.  The SQL itself is executed as a {@link PreparedStatement} with the given parameters.</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.findWithSQL(Person.class, "personID", "SELECT personID FROM chairs WHERE position &lt; ? LIMIT ?",
     * 10,
     * 5);</pre>
     *
     * <p>The SQL is not parsed or modified in any way by ActiveObjects.  As such, it is possible to execute
     * database-specific queries using this method without realizing it.  For example, the above query will not run on
     * MS SQL Server or Oracle, due to the lack of a LIMIT clause in their SQL implementation.  As such, be extremely
     * careful about what SQL is executed using this method, or else be conscious of the fact that you may be locking
     * yourself to a specific DBMS.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param keyField The field value to use in the creation of the entities.  This is usually the primary key field of
     * the corresponding table.
     * @param sql The SQL statement to execute.
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement.  The length of
     * this array <i>must</i> match the number of parameters (denoted by the '?' char) in the <code>criteria</code>.
     * @return An array of entities of the given type which match the specified query.
     */
    @SuppressWarnings("unchecked")
    public <T extends RawEntity<K>, K> T[] findWithSQL(Class<T> type, String keyField, String sql, Object... parameters) throws SQLException
    {
        List<T> back = new ArrayList<T>();
        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);

        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            connection = provider.getConnection();
            stmt = provider.preparedStatement(connection, sql);
            putStatementParameters(stmt, parameters);

            res = stmt.executeQuery();
            while (res.next())
            {
                back.add(peer(entityInfo, entityInfo.getPrimaryKey().getTypeInfo().getLogicalType().pullFromDatabase(this, res, (Class<K>) type, keyField)));
            }
        }
        finally
        {
            closeQuietly(res);
            closeQuietly(stmt);
            closeQuietly(connection);
        }

        return back.toArray((T[]) Array.newInstance(type, back.size()));
    }

    /**
     * <p>Opitimsed read for large datasets. This method will stream all rows for the given type to the given
     * callback.</p>
     *
     * <p>Please see {@link #stream(Class, Query, EntityStreamCallback)} for details / limitations.
     *
     * @param type The type of the entities to retrieve.
     * @param streamCallback The receiver of the data, will be passed one entity per returned row
     */
    public <T extends RawEntity<K>, K> void stream(Class<T> type, EntityStreamCallback<T, K> streamCallback) throws SQLException
    {
        stream(type, Query.select("*"), streamCallback);
    }

    /**
     * <p>Selects all entities of the given type and feeds them to the callback, one by one. The entities are slim,
     * read-only representations of the data. They only supports getters or designated {@link Accessor}
     * methods. Calling setters
     * or <pre>save</pre> will
     * result in an exception. Other method calls will be ignored. The proxies do not support lazy-loading of related
     * entities.</p>
     *
     * <p>Only the fields specified in the Query are loaded. Since lazy loading is not supported, calls to unspecified
     * getters will return null (or AO's defaults in case of primitives)</p>
     *
     * <p>This call is optimised for efficient read operations on large datasets. For best memory usage, do not buffer
     * the entities passed to the callback but process and discard them directly.</p>
     *
     * <p>Unlike regular Entities, the read only implementations do not support flushing/refreshing. The data is a
     * snapshot view at the time of query.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param query
     * @param streamCallback The receiver of the data, will be passed one entity per returned row
     */
    public <T extends RawEntity<K>, K> void stream(Class<T> type, Query query, EntityStreamCallback<T, K> streamCallback) throws SQLException
    {
        EntityInfo<T, K> entityInfo = resolveEntityInfo(type);

        query.resolvePrimaryKey(entityInfo.getPrimaryKey());

        final String[] canonicalFields = query.getCanonicalFields(entityInfo);

        // Execute the query
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            conn = provider.getConnection();
            final String sql = query.toSQL(entityInfo, provider, getTableNameConverter(), false);

            // we're only going over the result set once, so use the slimmest possible cursor type
            stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            provider.setQueryStatementProperties(stmt, query);

            query.setParameters(this, stmt);

            res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);

            while (res.next())
            {
                K primaryKey = entityInfo.getPrimaryKey().getTypeInfo().getLogicalType().pullFromDatabase(this, res, entityInfo.getPrimaryKey().getJavaType(), entityInfo.getPrimaryKey().getName());
                ReadOnlyEntityProxy<T, K> proxy = createReadOnlyProxy(entityInfo, primaryKey);
                T entity = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class[]{type}, proxy));

                // transfer the values from the result set into the local proxy value store. We're not caching the proxy itself anywhere, since
                // it's designated as a read-only snapshot view of the data and thus doesn't need flushing.
                for (String fieldName : canonicalFields)
                {
                    proxy.addValue(fieldName, res);
                }

                // forward the proxy to the callback for the client to consume
                streamCallback.onRowRead(entity);
            }
        }
        finally
        {
            closeQuietly(res, stmt, conn);
        }
    }

    private <T extends RawEntity<K>, K> ReadOnlyEntityProxy<T, K> createReadOnlyProxy(EntityInfo<T, K> entityInfo, K primaryKey)
    {
        return new ReadOnlyEntityProxy<T, K>(this, entityInfo, primaryKey);
    }

    /**
     * Counts all entities of the specified type.  This method is actually a delegate for: <code>count(Class&lt;?
     * extends Entity&gt;, Query)</code>
     *
     * @param type The type of the entities which should be counted.
     * @return The number of entities of the specified type.
     */
    public <K> int count(Class<? extends RawEntity<K>> type) throws SQLException
    {
        return count(type, Query.select());
    }

    /**
     * Counts all entities of the specified type matching the given criteria and parameters.  This is a convenience
     * method for: <code>count(type, Query.select().where(criteria, parameters))</code>
     *
     * @param type The type of the entities which should be counted.
     * @param criteria A parameterized WHERE statement used to determine the result set which will be counted.
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement.  The length of
     * this array <i>must</i> match the number of parameters (denoted by the '?' char) in the <code>criteria</code>.
     * @return The number of entities of the given type which match the specified criteria.
     */
    public <K> int count(Class<? extends RawEntity<K>> type, String criteria, Object... parameters) throws SQLException
    {
        return count(type, Query.select().where(criteria, parameters));
    }

    /**
     * Counts all entities of the specified type matching the given {@link Query} instance.  The SQL runs as a
     * <code>SELECT COUNT(*)</code> to ensure maximum performance.
     *
     * @param type The type of the entities which should be counted.
     * @param query The {@link Query} instance used to determine the result set which will be counted.
     * @return The number of entities of the given type which match the specified query.
     */
    public <K> int count(Class<? extends RawEntity<K>> type, Query query) throws SQLException
    {
        EntityInfo entityInfo = resolveEntityInfo(type);
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            connection = provider.getConnection();
            final String sql = query.toSQL(entityInfo, provider, getTableNameConverter(), true);

            stmt = provider.preparedStatement(connection, sql);
            provider.setQueryStatementProperties(stmt, query);

            query.setParameters(this, stmt);

            res = stmt.executeQuery();
            return res.next() ? res.getInt(1) : -1;

        }
        finally
        {
            closeQuietly(res);
            closeQuietly(stmt);
            closeQuietly(connection);
        }
    }

    public NameConverters getNameConverters()
    {
        return nameConverters;
    }

    protected <T extends RawEntity<K>, K> EntityInfo<T, K> resolveEntityInfo(Class<T> type) {
        return entityInfoResolver.resolve(type);
    }

    /**
     * Retrieves the {@link TableNameConverter} instance used for name conversion of all entity types.
     */
    public TableNameConverter getTableNameConverter()
    {
        return nameConverters.getTableNameConverter();
    }

    /**
     * Retrieves the {@link FieldNameConverter} instance used for name conversion of all entity methods.
     */
    public FieldNameConverter getFieldNameConverter()
    {
        return nameConverters.getFieldNameConverter();
    }

    /**
     * Specifies the {@link PolymorphicTypeMapper} instance to use for all flag value conversion of polymorphic types.
     * The default type mapper is an empty {@link DefaultPolymorphicTypeMapper} instance (thus using the fully qualified
     * classname for all values).
     *
     * @see #getPolymorphicTypeMapper()
     */
    public void setPolymorphicTypeMapper(PolymorphicTypeMapper typeMapper)
    {
        typeMapperLock.writeLock().lock();
        try
        {
            this.typeMapper = typeMapper;

            if (typeMapper instanceof DefaultPolymorphicTypeMapper)
            {
                ((DefaultPolymorphicTypeMapper)typeMapper).resolveMappings(getTableNameConverter());
            }
        }
        finally
        {
            typeMapperLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the {@link PolymorphicTypeMapper} instance used for flag value conversion of polymorphic types.
     *
     * @see #setPolymorphicTypeMapper(PolymorphicTypeMapper)
     */
    public PolymorphicTypeMapper getPolymorphicTypeMapper()
    {
        typeMapperLock.readLock().lock();
        try
        {
            if (typeMapper == null)
            {
                throw new RuntimeException("No polymorphic type mapper was specified");
            }

            return typeMapper;
        }
        finally
        {
            typeMapperLock.readLock().unlock();
        }
    }

    /**
     * <p>Retrieves the database provider used by this <code>EntityManager</code> for all database operations.  This
     * method can be used reliably to obtain a database provider and hence a {@link Connection} instance which can be
     * used for JDBC operations outside of ActiveObjects.  Thus:</p>
     *
     * <pre>Connection conn = manager.getProvider().getConnection();
     * try {
     *     // ...
     * } finally {
     *     conn.close();
     * }</pre>
     */
    public DatabaseProvider getProvider()
    {
        return provider;
    }

    <T extends RawEntity<K>, K> EntityProxy<T, K> getProxyForEntity(T entity) {
        return ((EntityProxyAccessor) entity).getEntityProxy();
    }

    private void verify(RawEntity<?> entity)
    {
        if (entity.getEntityManager() != this)
        {
            throw new RuntimeException("Entities can only be used with a single EntityManager instance");
        }
    }

    private void putStatementParameters(PreparedStatement stmt, Object... parameters) throws SQLException
    {
        for (int i = 0; i < parameters.length; ++i)
        {
            Object parameter = parameters[i];
            Class entityTypeOrClass = (parameter instanceof RawEntity) ? ((RawEntity)parameter).getEntityType() : parameter.getClass();
            @SuppressWarnings("unchecked") TypeInfo<Object> typeInfo = provider.getTypeManager().getType(entityTypeOrClass);
            typeInfo.getLogicalType().putToDatabase(this, stmt, i + 1, parameter, typeInfo.getJdbcWriteType());
        }
    }

    private static interface Function<R, F>
    {
        public R invoke(F formals) throws SQLException;
    }
}
