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

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.preloadValue;
import static net.java.ao.sql.SqlUtils.closeQuietly;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Iterables;
import net.java.ao.cache.Cache;
import net.java.ao.cache.CacheLayer;
import net.java.ao.cache.RAMCache;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.CachingNameConverters;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import net.java.ao.util.StringUtils;

/**
 * <p>The root control class for the entire ActiveObjects API.  <code>EntityManager</code>
 * is the source of all {@link RawEntity} objects, as well as the dispatch layer between the entities,
 * the pluggable table name converters, and the database abstraction layers.  This is the
 * entry point for any use of the API.</p>
 *
 * <p><code>EntityManager</code> is designed to be used in an instance fashion with each
 * instance corresponding to a single database.  Thus, rather than a singleton instance or a
 * static factory method, <code>EntityManager</code> does have a proper constructor.  Any
 * static instance management is left up to the developer using the API.</p>
 *
 * @author Daniel Spiewak
 */
public class EntityManager
{
    private final DatabaseProvider provider;
    private final EntityManagerConfiguration configuration;

    private final SchemaConfiguration schemaConfiguration;
    private final NameConverters nameConverters;
	private final ThreadLocal<Map<CacheKey<?>, Reference<RawEntity<?>>>> entityCache = new ThreadLocal<Map<CacheKey<?>, Reference<RawEntity<?>>>>() {

        @Override
        protected Map<CacheKey<?>, Reference<RawEntity<?>>> initialValue()
        {
            return new LRUMap<CacheKey<?>, Reference<RawEntity<?>>>(500);
        }

    };

	private Cache cache;
	private final ReadWriteLock cacheLock = new ReentrantReadWriteLock(true);

	private PolymorphicTypeMapper typeMapper;
	private final ReadWriteLock typeMapperLock = new ReentrantReadWriteLock(true);

	private Map<Class<? extends ValueGenerator<?>>, ValueGenerator<?>> valGenCache;
	private final ReadWriteLock valGenCacheLock = new ReentrantReadWriteLock(true);

    /**
     * Creates a new instance of <code>EntityManager</code> using the specified
     * {@link DatabaseProvider}.  This constructor initializes the entity and proxy
     * caches based on the given boolean value.  If <code>true</code>, the entities
     * will be weakly cached, not maintaining a reference allowing for garbage
     * collection.  If <code>false</code>, then strong caching will be used, preventing
     * garbage collection and ensuring the cache is logically complete.  If you are
     * concerned about memory, specify <code>true</code>.  Otherwise, for
     * maximum performance use <code>false</code> (highly recomended).
     *
     * @param provider the {@link DatabaseProvider} to use in all database operations.
     * @param configuration the configuration for this entity manager
     */
    public EntityManager(DatabaseProvider provider, EntityManagerConfiguration configuration)
    {
        this.provider = checkNotNull(provider);
        this.configuration = checkNotNull(configuration);
        cache = new RAMCache();
        valGenCache = new HashMap<Class<? extends ValueGenerator<?>>, ValueGenerator<?>>();

        // TODO: move caching out of there!
        nameConverters = new CachingNameConverters(configuration.getNameConverters());
        schemaConfiguration = checkNotNull(configuration.getSchemaConfiguration());

        typeMapper = new DefaultPolymorphicTypeMapper(new HashMap<Class<? extends RawEntity<?>>, String>());
    }


    /**
	 * Convenience method to create the schema for the specified entities
	 * using the current settings (table/field name converter and database provider).
	 *
	 * @param entities the "list" of entity classes to consider for migration.
     * @see SchemaGenerator#migrate(DatabaseProvider, SchemaConfiguration, NameConverters, Class[])
	 */
	public void migrate(Class<? extends RawEntity<?>>... entities) throws SQLException {
        SchemaGenerator.migrate(provider, schemaConfiguration, nameConverters, entities);
	}

	/**
	 * Flushes all value caches contained within entities controlled by this <code>EntityManager</code>
	 * instance. Rather, it simply dumps all of the field values cached within the entities
	 * themselves (with the exception of the primary key value).  This should be used in the case
	 * of a complex process outside AO control which may have changed values in the database.  If
	 * it is at all possible to determine precisely which rows have been changed, the {@link #flush(RawEntity[])} }
	 * method should be used instead.
	 */
	public void flushAll() {
		for (final Reference<RawEntity<?>> entityReference : entityCache.get().values()) {
            final RawEntity<?> entity = entityReference.get();
            if (entity != null) {
                ((EntityProxyAccessor) entity).getEntityProxy().flushCache(entity);
            }
		}

        entityCache.get().clear();
    }

    /**
     * Flush the current thread's entity cache. Should be called after a transaction is committed or rolled back.
     */
    public void flushEntityCache() {
        entityCache.get().clear();
    }

	/**
	 * Flushes the value caches of the specified entities along with all of the relevant
	 * relations cache entries.  This should be called after a process outside of AO control
	 * may have modified the values in the specified rows.  This does not actually remove
	 * the entity instances themselves from the instance cache.  Rather, it just flushes all
	 * of their internally cached values (with the exception of the primary key).
	 */
	public void flush(RawEntity<?>... entities) {
		Map<RawEntity<?>, EntityProxy<?, ?>> toFlush = new HashMap<RawEntity<?>, EntityProxy<?, ?>>();
        for (RawEntity<?> entity : entities) {
            verify(entity);
            toFlush.put(entity, getProxyForEntity(entity));
        }
        for (Entry<RawEntity<?>, EntityProxy<?, ?>> entry : toFlush.entrySet()) {
			entry.getValue().flushCache(entry.getKey());
		}
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
	public final <T extends RawEntity<K>, K> T[] get(final Class<T> type, K... keys) throws SQLException
    {
		final String primaryKeyField = Common.getPrimaryKeyField(type, getFieldNameConverter());
		return getFromCache(type, findByPrimaryKey(type, primaryKeyField), keys);
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

    protected <T extends RawEntity<K>, K> T[] peer(final Class<T> type, K... keys) throws SQLException
    {
		return getFromCache(type, new Function<T, K>() {
			public T invoke(K key) {
				return getAndInstantiate(type, key);
			}
		}, keys);
	}

	private <T extends RawEntity<K>, K> T[] getFromCache(Class<T> type, Function<T, K> create, K... keys) throws SQLException
    {
		T[] back = (T[]) Array.newInstance(type, keys.length);
		int index = 0;

		for (K key : keys) {
            Reference<?> reference = entityCache.get().get(new CacheKey<K>(key, type)); // upcast to workaround bug in javac
            Reference<T> ref = (Reference<T>) reference;
            T entity = (ref == null ? null : ref.get());

            if (entity != null) {
                back[index++] = entity;
            } else {
                back[index++] = create.invoke(key);
            }
        }

		return back;
	}

	/**
	 * Creates a new instance of the entity of the specified type corresponding to the
	 * given primary key.  This is used by {@link #get(Class, Object[])}} to create the entity
	 * if the instance is not found already in the cache.  This method should not be
	 * repurposed to perform any caching, since ActiveObjects already assumes that
	 * the caching has been performed.
	 *
	 *  @param type	The type of the entity to create.
	 *  @param key		The primary key corresponding to the entity instance required.
	 *  @return An entity instance of the specified type and primary key.
	 */
	protected <T extends RawEntity<K>, K> T getAndInstantiate(Class<T> type, K key) {
		EntityProxy<T, K> proxy = new EntityProxy<T, K>(this, type, key);

		T entity = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type, EntityProxyAccessor.class}, proxy));
		entityCache.get().put(new CacheKey<K>(key, type), createRef(entity));
		return entity;
	}

	/**
	 * Cleverly overloaded method to return a single entity of the specified type
	 * rather than an array in the case where only one ID is passed.  This method
	 * meerly delegates the call to the overloaded <code>get</code> method
	 * and functions as syntactical sugar.
	 *
	 * @param type		The type of the entity instance to retrieve.
	 * @param key		The primary key corresponding to the entity to be retrieved.
	 * @return An entity instance of the given type corresponding to the specified
	 * 		primary key, or <code>null</code> if the entity does not exist in the database.
	 * @see #get(Class, Object[])
	 */
	public <T extends RawEntity<K>, K> T get(Class<T> type, K key) throws SQLException
    {
        return get(type, toArray(key))[0];
	}

    protected <T extends RawEntity<K>, K> T peer(Class<T> type, K key) throws SQLException
    {
        return peer(type, toArray(key))[0];
	}

    @SuppressWarnings("unchecked")
    private static <K> K[] toArray(K key)
    {
        return (K[]) new Object[]{key};
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
		final String table = nameConverters.getTableNameConverter().getName(type);

		Set<DBParam> listParams = new HashSet<DBParam>();
		listParams.addAll(Arrays.asList(params));

        for (Method method : MethodFinder.getInstance().findAnnotatedMethods(Generator.class, type)) {
            Generator genAnno = method.getAnnotation(Generator.class);
            final String field = nameConverters.getFieldNameConverter().getName(method);
            ValueGenerator<?> generator;

            valGenCacheLock.writeLock().lock();
            try {
                if (valGenCache.containsKey(genAnno.value())) {
                    generator = valGenCache.get(genAnno.value());
                } else {
                    generator = genAnno.value().newInstance();
                    valGenCache.put(genAnno.value(), generator);
                }
            } catch (InstantiationException e) {
                continue;
            } catch (IllegalAccessException e) {
                continue;
            } finally {
                valGenCacheLock.writeLock().unlock();
            }

            listParams.add(new DBParam(field, generator.generateValue(this)));
        }

        final Method pkMethod = Common.getPrimaryKeyMethod(type);
        final String pkField = Common.getPrimaryKeyField(type, getFieldNameConverter());

        final Set<String> nonNullFields = Common.getNonNullFields(type, nameConverters.getFieldNameConverter());
        final Set<String> fieldsThatShouldBeSet = Common.getNonNullFieldsWithNoDefaultAndNotGenerated(type, nameConverters.getFieldNameConverter());
        final Map<String, TypeInfo> valueFields = Common.getValueFields(provider.getTypeManager(), nameConverters.getFieldNameConverter(), type);

        for (DBParam param : params)
        {
            if (param.getField().equals(pkField))
            {
                Common.validatePrimaryKey(provider.getTypeManager(),type,param.getValue());
            }
            else if (nonNullFields.contains(param.getField()) && param.getValue() == null)
            {
                throw new IllegalArgumentException("Cannot set non-null field " + param.getField() + " to null");
            }
            else if (nonNullFields.contains(param.getField()) && param.getValue() instanceof String && StringUtils.isBlank((String) param.getValue()))
            {
                throw new IllegalArgumentException("Cannot set non-null String field " + param.getField() + " to ''");
            }

            fieldsThatShouldBeSet.remove(param.getField());

            final TypeInfo dbType = valueFields.get(param.getField());
            if (dbType != null && param.getValue() != null)
            {
                dbType.getLogicalType().validate(param.getValue());
            }
        }

        if (!fieldsThatShouldBeSet.isEmpty())
        {
            throw new IllegalArgumentException("There are some non-null fields that weren't set when trying to create entity '" + type.getName() + "', those fields are: " + nonNullFields);
        }

		Connection connection = null;
		try
        {
            connection = provider.getConnection();
			back = peer(type, provider.insertReturningKey(this, connection,
                    type,
					Common.getPrimaryKeyClassType(type),
					Common.getPrimaryKeyField(type, getFieldNameConverter()),
					pkMethod.getAnnotation(AutoIncrement.class) != null, table, listParams.toArray(new DBParam[listParams.size()])));
		}
        finally
        {
            closeQuietly(connection);
        }
		back.init();
		return back;
	}

	/**
	 * Creates and INSERTs a new entity of the specified type with the given map of
	 * parameters.  This method merely delegates to the {@link #create(Class, DBParam...)}
	 * method.  The idea behind having a separate convenience method taking a map is in
	 * circumstances with large numbers of parameters or for people familiar with the
	 * anonymous inner class constructor syntax who might be more comfortable with
	 * creating a map than with passing a number of objects.
	 *
	 * @param type	The type of the entity to INSERT.
	 * @param params	A map of parameters to pass to the INSERT.
	 * @return	The new entity instance corresponding to the INSERTed row.
	 * @see #create(Class, DBParam...)
	 */
	public <T extends RawEntity<K>, K> T create(Class<T> type, Map<String, Object> params) throws SQLException {
		DBParam[] arrParams = new DBParam[params.size()];
		int i = 0;

		for (String key : params.keySet()) {
			arrParams[i++] = new DBParam(key, params.get(key));
		}

		return create(type, arrParams);
	}

	/**
	 * <p>Deletes the specified entities from the database.  DELETE statements are
	 * called on the rows in the corresponding tables and the entities are removed
	 * from the instance cache.  The entity instances themselves are not invalidated,
	 * but it doesn't even make sense to continue using the instance without a row
	 * with which it is paired.</p>
	 *
	 * <p>This method does attempt to group the DELETE statements on a per-type
	 * basis.  Thus, if you pass 5 instances of <code>EntityA</code> and two
	 * instances of <code>EntityB</code>, the following SQL prepared statements
	 * will be invoked:</p>
	 *
	 * <pre>DELETE FROM entityA WHERE id IN (?,?,?,?,?);
	 * DELETE FROM entityB WHERE id IN (?,?);</pre>
	 *
	 * <p>Thus, this method scales very well for large numbers of entities grouped
	 * into types.  However, the execution time increases linearly for each entity of
	 * unique type.</p>
	 *
	 * @param entities	A varargs array of entities to delete.  Method returns immediately
	 * 	if length == 0.
	 */
	@SuppressWarnings("unchecked")
	public void delete(RawEntity<?>... entities) throws SQLException {
		if (entities.length == 0) {
			return;
		}

		Map<Class<? extends RawEntity<?>>, List<RawEntity<?>>> organizedEntities =
			new HashMap<Class<? extends RawEntity<?>>, List<RawEntity<?>>>();

		for (RawEntity<?> entity : entities) {
			verify(entity);
			Class<? extends RawEntity<?>> type = getProxyForEntity(entity).getType();

			if (!organizedEntities.containsKey(type)) {
				organizedEntities.put(type, new LinkedList<RawEntity<?>>());
			}
			organizedEntities.get(type).add(entity);
		}

        Connection conn = null;
        PreparedStatement stmt = null;
        try
        {
            conn = provider.getConnection();
            for (Class<? extends RawEntity<?>> type : organizedEntities.keySet()) {
                List<RawEntity<?>> entityList = organizedEntities.get(type);

                StringBuilder sql = new StringBuilder("DELETE FROM ");
                sql.append(provider.withSchema(nameConverters.getTableNameConverter().getName(type)));
                sql.append(" WHERE ").append(provider.processID(
                        Common.getPrimaryKeyField(type, getFieldNameConverter()))).append(" IN (?");

                for (int i = 1; i < entityList.size(); i++) {
                    sql.append(",?");
                }
                sql.append(')');

                stmt = provider.preparedStatement(conn, sql);

                int index = 1;
                for (RawEntity<?> entity : entityList) {
                    TypeInfo typeInfo = provider.getTypeManager().getType(entity.getEntityType());
                    typeInfo.getLogicalType().putToDatabase(this, stmt, index++, entity, typeInfo.getJdbcWriteType());
                }
                stmt.executeUpdate();
            }
        }
        finally
        {
            closeQuietly(stmt);
            closeQuietly(conn);
        }

        for (RawEntity<?> entity : entities) {
            entityCache.get().remove(new CacheKey(Common.getPrimaryKeyValue(entity), entity.getEntityType()));
        }
    }

    /**
     * <p>Deletes rows from the table corresponding to {@code type}. In contrast to {@link #delete(RawEntity[])},
     * this method allows you to delete rows without creating entities for them first.</p>
     *
     * <p>Example:</p>
     *
     * <pre>manager.deleteWithSQL(Person.class, "name = ?", "Charlie")</pre>
     *
     * <p>The SQL in {@code criteria} is not parsed or modified in any way by ActiveObjects, and is simply appended
     * to the DELETE statement in a WHERE clause. The above example would cause an SQL statement similar to the
     * following to be executed:</p>
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
     * @param parameters A varargs array of parameters to be passed to the executed prepared statement. The length
     * of this array <i>must</i> match the number of parameters (denoted by the '?' char) in {@code criteria}.
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
            sql.append(criteria);
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
	 * Returns all entities of the given type.  This actually peers the call to
	 * the {@link #find(Class, Query)} method.
	 *
	 * @param type		The type of entity to retrieve.
	 * @return	An array of all entities which correspond to the given type.
	 */
	public <T extends RawEntity<K>, K> T[] find(Class<T> type) throws SQLException {
		return find(type, Query.select());
	}

	/**
	 * <p>Convenience method to select all entities of the given type with the
	 * specified, parameterized criteria.  The <code>criteria</code> String
	 * specified is appended to the SQL prepared statement immediately
	 * following the <code>WHERE</code>.</p>
	 *
	 * <p>Example:</p>
	 *
	 * <pre>manager.find(Person.class, "name LIKE ? OR age &gt; ?", "Joe", 9);</pre>
	 *
	 * <p>This actually delegates the call to the {@link #find(Class, Query)}
	 * method, properly parameterizing the {@link Query} object.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param criteria		A parameterized WHERE statement used to determine the results.
	 * @param parameters	A varargs array of parameters to be passed to the executed
	 * 	prepared statement.  The length of this array <i>must</i> match the number of
	 * 	parameters (denoted by the '?' char) in the <code>criteria</code>.
	 * @return	An array of entities of the given type which match the specified criteria.
	 */
	public final <T extends RawEntity<K>, K> T[] find(Class<T> type, String criteria, Object... parameters) throws SQLException {
		return find(type, Query.select().where(criteria, parameters));
	}

    /**
	 * <p>Convenience method to select a single entity of the given type with the
	 * specified, parameterized criteria.  The <code>criteria</code> String
	 * specified is appended to the SQL prepared statement immediately
	 * following the <code>WHERE</code>.</p>
	 *
	 * <p>Example:</p>
	 *
	 * <pre>manager.findSingleEntity(Person.class, "name LIKE ? OR age &gt; ?", "Joe", 9);</pre>
	 *
	 * <p>This actually delegates the call to the {@link #find(Class, String, Object...)}
	 * method, properly parameterizing the {@link Object} object.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param criteria		A parameterized WHERE statement used to determine the results.
	 * @param parameters	A varargs array of parameters to be passed to the executed
	 * 	prepared statement.  The length of this array <i>must</i> match the number of
	 * 	parameters (denoted by the '?' char) in the <code>criteria</code>.
	 * @return	A single entity of the given type which match the specified criteria or null if none returned
	 */
    public <T extends RawEntity<K>, K> T findSingleEntity(Class<T> type, String criteria, Object... parameters) throws SQLException {
        T[] entities = find(type, criteria, parameters);

        if (entities.length < 1) {
            return null;
        } else if (entities.length > 1) {
            throw new IllegalStateException("Found more than one entities of type '"
                    + type.getSimpleName() + "' that matched the criteria '" + criteria
                    + "' and parameters '" + parameters.toString() + "'.");
        }

        return entities[0];
    }

	/**
	 * <p>Selects all entities matching the given type and {@link Query}.  By default, the
	 * entities will be created based on the values within the primary key field for the
	 * specified type (this is usually the desired behavior).</p>
	 *
	 * <p>Example:</p>
	 *
	 * <pre>manager.find(Person.class, Query.select().where("name LIKE ? OR age &gt; ?", "Joe", 9).limit(10));</pre>
	 *
	 * <p>This method delegates the call to {@link #find(Class, String, Query)}, passing the
	 * primary key field for the given type as the <code>String</code> parameter.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param query	The {@link Query} instance to be used to determine the results.
	 * @return An array of entities of the given type which match the specified query.
	 */
	public final <T extends RawEntity<K>, K> T[] find(Class<T> type, Query query) throws SQLException {
		String selectField = Common.getPrimaryKeyField(type, getFieldNameConverter());
		query.resolveFields(type, getFieldNameConverter());

		Iterable<String> fields = query.getFields();
		if (Iterables.size(fields) == 1) {
			selectField = Iterables.get(fields, 0);
		}

		return find(type, selectField, query);
	}

	/**
	 * <p>Selects all entities of the specified type which match the given
	 * <code>Query</code>.  This method creates a <code>PreparedStatement</code>
	 * using the <code>Query</code> instance specified against the table
	 * represented by the given type.  This query is then executed (with the
	 * parameters specified in the query).  The method then iterates through
	 * the result set and extracts the specified field, mapping an entity
	 * of the given type to each row.  This array of entities is returned.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param field		The field value to use in the creation of the entities.  This is usually
	 * 	the primary key field of the corresponding table.
	 * @param query	The {@link Query} instance to use in determining the results.
	 * @return	An array of entities of the given type which match the specified query.
	 */
	public final <T extends RawEntity<K>, K> T[] find(Class<T> type, String field, Query query) throws SQLException {
		List<T> back = new ArrayList<T>();

		query.resolveFields(type, getFieldNameConverter());

		final Preload preloadAnnotation = type.getAnnotation(Preload.class);
		if (preloadAnnotation != null) {
			if (!Iterables.get(query.getFields(),0).equals("*") && query.getJoins().isEmpty()) {
				Iterable<String> oldFields = query.getFields();
				List<String> newFields = new ArrayList<String>();

				for (String newField : preloadValue(preloadAnnotation, nameConverters.getFieldNameConverter())) {
					newField = newField.trim();

					int fieldLoc = -1;
					for (int i = 0; i < Iterables.size(oldFields); i++) {
						if (Iterables.get(oldFields, i).equals(newField)) {
							fieldLoc = i;
							break;
						}
					}

					if (fieldLoc < 0) {
						newFields.add(newField);
					} else {
						newFields.add(Iterables.get(oldFields, fieldLoc));
					}
				}

				if (!newFields.contains("*")) {
					for (String oldField : oldFields) {
						if (!newFields.contains(oldField)) {
							newFields.add(oldField);
						}
					}
				}

				query.setFields(newFields.toArray(new String[newFields.size()]));
			}
		}

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            conn = provider.getConnection();
            final String sql = query.toSQL(type, provider, getTableNameConverter(), getFieldNameConverter(), false);

            stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            provider.setQueryStatementProperties(stmt, query);

            query.setParameters(this, stmt);

            res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);

            final TypeInfo<K> primaryKeyType = Common.getPrimaryKeyType(provider.getTypeManager(), type);
            final Class<K> primaryKeyClassType = Common.getPrimaryKeyClassType(type);
            final String[] canonicalFields = query.getCanonicalFields(provider, getFieldNameConverter(), type);
            while (res.next())
            {
                final T entity = peer(type, primaryKeyType.getLogicalType().pullFromDatabase(this, res, primaryKeyClassType, field));
                final CacheLayer cacheLayer = getProxyForEntity(entity).getCacheLayer(entity);

                for (String cacheField : canonicalFields)
                {
                    cacheLayer.put(cacheField, res.getObject(cacheField));
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
	 * <p>Executes the specified SQL and extracts the given key field, wrapping each
	 * row into a instance of the specified type.  The SQL itself is executed as
	 * a {@link PreparedStatement} with the given parameters.</p>
	 *
	 *  <p>Example:</p>
	 *
	 *  <pre>manager.findWithSQL(Person.class, "personID", "SELECT personID FROM chairs WHERE position &lt; ? LIMIT ?", 10, 5);</pre>
	 *
	 *  <p>The SQL is not parsed or modified in any way by ActiveObjects.  As such, it is
	 *  possible to execute database-specific queries using this method without realizing
	 *  it.  For example, the above query will not run on MS SQL Server or Oracle, due to
	 *  the lack of a LIMIT clause in their SQL implementation.  As such, be extremely
	 *  careful about what SQL is executed using this method, or else be conscious of the
	 *  fact that you may be locking yourself to a specific DBMS.</p>
	 *
	 * @param type		The type of the entities to retrieve.
	 * @param keyField	The field value to use in the creation of the entities.  This is usually
	 * 	the primary key field of the corresponding table.
	 * @param sql	The SQL statement to execute.
	 * @param parameters	A varargs array of parameters to be passed to the executed
	 * 	prepared statement.  The length of this array <i>must</i> match the number of
	 * 	parameters (denoted by the '?' char) in the <code>criteria</code>.
	 * @return	An array of entities of the given type which match the specified query.
	 */
	@SuppressWarnings("unchecked")
	public <T extends RawEntity<K>, K> T[] findWithSQL(Class<T> type, String keyField, String sql, Object... parameters) throws SQLException {
		List<T> back = new ArrayList<T>();

        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try
        {
            connection = provider.getConnection();
			stmt = provider.preparedStatement(connection, sql);

			final TypeManager manager = provider.getTypeManager();
			for (int i = 0; i < parameters.length; i++) {
				Class javaType = parameters[i].getClass();

				if (parameters[i] instanceof RawEntity) {
					javaType = ((RawEntity<?>) parameters[i]).getEntityType();
				}

				TypeInfo<Object> typeInfo = manager.getType(javaType);
				typeInfo.getLogicalType().putToDatabase(this, stmt, i + 1, parameters[i], typeInfo.getJdbcWriteType());
			}

			res = stmt.executeQuery();
			while (res.next()) {
				back.add(peer(type, Common.getPrimaryKeyType(provider.getTypeManager(), type).getLogicalType().pullFromDatabase(this, res, (Class<K>)type, keyField)));
			}
		} finally {
            closeQuietly(res);
			closeQuietly(stmt);
			closeQuietly(connection);
		}

		return back.toArray((T[]) Array.newInstance(type, back.size()));
	}

	/**
	 * <p>Opitimsed read for large datasets. This method will stream all rows for the given type to the given callback.</p>
	 *
	 * <p>Please see {@link #stream(Class, Query, EntityStreamCallback)} for details / limitations.
	 *
	 * @param type The type of the entities to retrieve.
	 * @param streamCallback The receiver of the data, will be passed one entity per returned row
	 */
	public <T extends RawEntity<K>, K> void stream(Class<T> type, EntityStreamCallback<T, K> streamCallback) throws SQLException {
	    stream(type, Query.select("*"), streamCallback);
	}

    /**
     * <p>Selects all entities of the given type and feeds them to the callback, one by one. The entities are slim, uncached, read-only
     * representations of the data. They only supports getters or designated {@link Accessor} methods. Calling setters or <pre>save</pre> will
     * result in an exception. Other method calls will be ignored. The proxies do not support lazy-loading of related entities.</p>
     *
     * <p>Only the fields specified in the Query are loaded. Since lazy loading is not supported, calls to unspecified getters will return null
     * (or AO's defaults in case of primitives)</p>
     *
     * <p>This call is optimised for efficient read operations on large datasets. For best memory usage, do not buffer the entities passed to the
     * callback but process and discard them directly.</p>
     *
     * <p>Unlike regular Entities, the read only implementations do not support flushing/refreshing. The data is a snapshot view at the time of
     * query.</p>
     *
     * @param type The type of the entities to retrieve.
     * @param query
     * @param streamCallback The receiver of the data, will be passed one entity per returned row
     */
    public <T extends RawEntity<K>, K> void stream(Class<T> type, Query query, EntityStreamCallback<T, K> streamCallback) throws SQLException {

        query.resolveFields(type, getFieldNameConverter());

        // fetch some information about the fields we're dealing with. These calls are expensive when
        // executed too often, and since we're always working on the same type of object, we only need them once.
        final TypeInfo<K> primaryKeyType = Common.getPrimaryKeyType(provider.getTypeManager(), type);
        final Class<K> primaryKeyClassType = Common.getPrimaryKeyClassType(type);
        final String[] canonicalFields = query.getCanonicalFields(provider, getFieldNameConverter(), type);
        String field = Common.getPrimaryKeyField(type, getFieldNameConverter());

        // the factory caches details about the proxied interface, since reflection calls are expensive
        // inside the stream loop
        ReadOnlyEntityProxyFactory<T, K> proxyFactory = new ReadOnlyEntityProxyFactory<T, K>(this, type);
        
        // Execute the query
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        try {
            conn = provider.getConnection();
            final String sql = query.toSQL(type, provider, getTableNameConverter(), getFieldNameConverter(), false);
            
            // we're only going over the result set once, so use the slimmest possible cursor type
            stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            provider.setQueryStatementProperties(stmt, query);
            
            query.setParameters(this, stmt);
            
            res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);
            
            while (res.next())
            {
                K primaryKey = primaryKeyType.getLogicalType().pullFromDatabase(this, res, primaryKeyClassType, field);
                // use the cached instance information from the factory to build efficient, read-only proxy representations
                ReadOnlyEntityProxy<T, K> proxy = proxyFactory.build(primaryKey);
                T entity = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type, EntityProxyAccessor.class}, proxy));
                
                // transfer the values from the result set into the local proxy cache. We're not caching the proxy itself anywhere, since
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

	/**
	 * Counts all entities of the specified type.  This method is actually
	 * a delegate for: <code>count(Class&lt;? extends Entity&gt;, Query)</code>
	 *
	 * @param type		The type of the entities which should be counted.
	 * @return The number of entities of the specified type.
	 */
	public <K> int count(Class<? extends RawEntity<K>> type) throws SQLException {
		return count(type, Query.select());
	}

	/**
	 * Counts all entities of the specified type matching the given criteria
	 * and parameters.  This is a convenience method for:
	 * <code>count(type, Query.select().where(criteria, parameters))</code>
	 *
	 * @param type		The type of the entities which should be counted.
	 * @param criteria		A parameterized WHERE statement used to determine the result
	 * 	set which will be counted.
	 * @param parameters	A varargs array of parameters to be passed to the executed
	 * 	prepared statement.  The length of this array <i>must</i> match the number of
	 * 	parameters (denoted by the '?' char) in the <code>criteria</code>.
	 * @return The number of entities of the given type which match the specified criteria.
	 */
	public <K> int count(Class<? extends RawEntity<K>> type, String criteria, Object... parameters) throws SQLException {
		return count(type, Query.select().where(criteria, parameters));
	}

	/**
	 * Counts all entities of the specified type matching the given {@link Query}
	 * instance.  The SQL runs as a <code>SELECT COUNT(*)</code> to
	 * ensure maximum performance.
	 *
	 * @param type		The type of the entities which should be counted.
	 * @param query	The {@link Query} instance used to determine the result set which
	 * 	will be counted.
	 * @return The number of entities of the given type which match the specified query.
	 */
	public <K> int count(Class<? extends RawEntity<K>> type, Query query) throws SQLException
    {
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet res =null;
		try {
            connection = provider.getConnection();
			final String sql = query.toSQL(type, provider, getTableNameConverter(), getFieldNameConverter(), true);

			stmt = provider.preparedStatement(connection, sql);
			provider.setQueryStatementProperties(stmt, query);

			query.setParameters(this, stmt);

			res = stmt.executeQuery();
            return res.next() ? res.getInt(1) : -1;

		} finally {
            closeQuietly(res);
            closeQuietly(stmt);
            closeQuietly(connection);
		}
	}

    public NameConverters getNameConverters()
    {
        return nameConverters;
    }

	/**
	 * Retrieves the {@link TableNameConverter} instance used for name
	 * conversion of all entity types.
	 */
    public TableNameConverter getTableNameConverter() {
        return nameConverters.getTableNameConverter();
	}

	/**
	 * Retrieves the {@link FieldNameConverter} instance used for name
	 * conversion of all entity methods.
	 */
    public FieldNameConverter getFieldNameConverter() {
        return nameConverters.getFieldNameConverter();
	}

	/**
	 * Specifies the {@link PolymorphicTypeMapper} instance to use for
	 * all flag value conversion of polymorphic types.  The default type
	 * mapper is an empty {@link DefaultPolymorphicTypeMapper} instance
	 * (thus using the fully qualified classname for all values).
	 *
	 * @see #getPolymorphicTypeMapper()
	 */
	public void setPolymorphicTypeMapper(PolymorphicTypeMapper typeMapper) {
		typeMapperLock.writeLock().lock();
		try {
			this.typeMapper = typeMapper;

			if (typeMapper instanceof DefaultPolymorphicTypeMapper) {
				((DefaultPolymorphicTypeMapper) typeMapper).resolveMappings(getTableNameConverter());
			}
		} finally {
			typeMapperLock.writeLock().unlock();
		}
	}

	/**
	 * Retrieves the {@link PolymorphicTypeMapper} instance used for flag
	 * value conversion of polymorphic types.
	 *
	 * @see #setPolymorphicTypeMapper(PolymorphicTypeMapper)
	 */
	public PolymorphicTypeMapper getPolymorphicTypeMapper() {
		typeMapperLock.readLock().lock();
		try {
			if (typeMapper == null) {
				throw new RuntimeException("No polymorphic type mapper was specified");
			}

			return typeMapper;
		} finally {
			typeMapperLock.readLock().unlock();
		}
	}

	/**
	 * Sets the cache implementation to be used by all entities
	 * controlled by this manager.  Note that this only affects
	 * <i>new</i> entities that have not yet been instantiated
	 * (may pre-exist as rows in the database).  All old entities
	 * will continue to use the prior cache.
	 */
	public void setCache(Cache cache) {
		cacheLock.writeLock().lock();
		try {
			if (!this.cache.equals(cache)) {
				this.cache.dispose();
				this.cache = cache;
			}
		} finally {
			cacheLock.writeLock().unlock();
		}
	}

	public Cache getCache() {
		cacheLock.readLock().lock();
		try {
			return cache;
		} finally {
			cacheLock.readLock().unlock();
		}
	}

	/**
	 * <p>Retrieves the database provider used by this <code>EntityManager</code>
	 * for all database operations.  This method can be used reliably to obtain
	 * a database provider and hence a {@link Connection} instance which can
	 * be used for JDBC operations outside of ActiveObjects.  Thus:</p>
	 *
	 * <pre>Connection conn = manager.getProvider().getConnection();
	 * try {
	 *     // ...
	 * } finally {
	 *     conn.close();
	 * }</pre>
	 */
	public DatabaseProvider getProvider() {
		return provider;
	}

    <T extends RawEntity<K>, K> EntityProxy<T, K> getProxyForEntity(T entity) {
        return ((EntityProxyAccessor) entity).getEntityProxy();
    }

	private Reference<RawEntity<?>> createRef(RawEntity<?> entity) {
		if (configuration.useWeakCache()) {
			return new WeakReference<RawEntity<?>>(entity);
		}

		return new SoftReference<RawEntity<?>>(entity);
	}

	private void verify(RawEntity<?> entity) {
		if (entity.getEntityManager() != this) {
			throw new RuntimeException("Entities can only be used with a single EntityManager instance");
		}
	}

    private void putStatementParameters(PreparedStatement stmt, Object... parameters) throws SQLException
    {
        for (int i = 0; i < parameters.length; ++i)
        {
            Object parameter = parameters[i];
            Class entityTypeOrClass = (parameter instanceof RawEntity) ? ((RawEntity) parameter).getEntityType() : parameter.getClass();
            @SuppressWarnings("unchecked") TypeInfo<Object> typeInfo = provider.getTypeManager().getType(entityTypeOrClass);
            typeInfo.getLogicalType().putToDatabase(this, stmt, i + 1, parameter, typeInfo.getJdbcWriteType());
        }
    }

	private static interface Function<R, F> {
		public R invoke(F formals) throws SQLException;
	}
	
	private static class CacheKey<T> {
		private T key;
		private Class<? extends RawEntity<?>> type;
		
		public CacheKey(T key, Class<? extends RawEntity<T>> type) {
			this.key = key;
			this.type = type;
		}
		
		@Override
		public int hashCode() {
			return (type.hashCode() + key.hashCode()) % (2 << 15);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			
			if (obj instanceof CacheKey<?>) {
				CacheKey<T> keyObj = (CacheKey<T>) obj;
				
				if (key.equals(keyObj.key) && type.equals(keyObj.type)) {
					return true;
				}
			}
			
			return false;
		}
	}
}
