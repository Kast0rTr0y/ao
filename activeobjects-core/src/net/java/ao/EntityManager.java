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

import net.java.ao.cache.Cache;
import net.java.ao.cache.CacheLayer;
import net.java.ao.cache.RAMCache;
import net.java.ao.cache.RAMRelationsCache;
import net.java.ao.cache.RelationsCache;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.CachingTableNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

import com.google.common.collect.MapMaker;

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

    private final TableNameConverter tableNameConverter;
	private final FieldNameConverter fieldNameConverter;
    private final SchemaConfiguration schemaConfiguration;

	private Map<RawEntity<?>, EntityProxy<? extends RawEntity<?>, ?>> proxies;
	private final ReadWriteLock proxyLock = new ReentrantReadWriteLock(true);

	private Map<CacheKey<?>, Reference<RawEntity<?>>> entityCache;
	private final ReadWriteLock entityCacheLock = new ReentrantReadWriteLock(true);

	private Cache cache;
	private final ReadWriteLock cacheLock = new ReentrantReadWriteLock(true);

	private PolymorphicTypeMapper typeMapper;
	private final ReadWriteLock typeMapperLock = new ReentrantReadWriteLock(true);

	private Map<Class<? extends ValueGenerator<?>>, ValueGenerator<?>> valGenCache;
	private final ReadWriteLock valGenCacheLock = new ReentrantReadWriteLock(true);

	private final RelationsCache relationsCache = new RAMRelationsCache();

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
     * @param provider    the {@link DatabaseProvider} to use in all database operations.
     * @param configuration the configuration for this entity manager
	 */
	public EntityManager(DatabaseProvider provider, EntityManagerConfiguration configuration)
    {
        this.provider = checkNotNull(provider);
        this.configuration = checkNotNull(configuration);

        if (configuration.useWeakCache())
        {
            proxies = new MapMaker().weakKeys().makeMap();
        }
        else
        {
            proxies = new MapMaker().softKeys().makeMap();
        }

		entityCache = new LRUMap<CacheKey<?>, Reference<RawEntity<?>>>(500);
		cache = new RAMCache();
		valGenCache = new HashMap<Class<? extends ValueGenerator<?>>, ValueGenerator<?>>();

		tableNameConverter = new CachingTableNameConverter(checkNotNull(configuration.getTableNameConverter()));
		fieldNameConverter = checkNotNull(configuration.getFieldNameConverter());
        schemaConfiguration = checkNotNull(configuration.getSchemaConfiguration());

		typeMapper = new DefaultPolymorphicTypeMapper(new HashMap<Class<? extends RawEntity<?>>, String>());
	}


	/**
	 * Convenience method to create the schema for the specified entities
	 * using the current settings (table/field name converter and database provider).
	 *
	 * @param entities the "list" of entity classes to consider for migration.
     * @see SchemaGenerator#migrate(DatabaseProvider, SchemaConfiguration, net.java.ao.schema.TableNameConverter, net.java.ao.schema.FieldNameConverter, Class[])
	 */
	public void migrate(Class<? extends RawEntity<?>>... entities) throws SQLException {
        SchemaGenerator.migrate(provider, schemaConfiguration, tableNameConverter, fieldNameConverter, entities);
	}

	/**
	 * Flushes all value caches contained within entities controlled by this <code>EntityManager</code>
	 * instance.  This does not actually remove the entities from the instance cache maintained
	 * within this class.  Rather, it simply dumps all of the field values cached within the entities
	 * themselves (with the exception of the primary key value).  This should be used in the case
	 * of a complex process outside AO control which may have changed values in the database.  If
	 * it is at all possible to determine precisely which rows have been changed, the {@link #flush(RawEntity[])} }
	 * method should be used instead.
	 */
	public void flushAll() {
		List<Map.Entry<RawEntity<?>, EntityProxy<? extends RawEntity<?>, ?>>> toFlush = new LinkedList<Map.Entry<RawEntity<?>, EntityProxy<? extends RawEntity<?>, ?>>>();

		proxyLock.readLock().lock();
		try {
			for (Map.Entry<RawEntity<?>, EntityProxy<? extends RawEntity<?>, ?>> proxy : proxies.entrySet()) {
				toFlush.add(proxy);
			}
		} finally {
			proxyLock.readLock().unlock();
		}

		for (Map.Entry<RawEntity<?>, EntityProxy<? extends RawEntity<?>, ?>> entry : toFlush) {
			entry.getValue().flushCache(entry.getKey());
		}

		relationsCache.flush();
	}

	/**
	 * Flushes the value caches of the specified entities along with all of the relevant
	 * relations cache entries.  This should be called after a process outside of AO control
	 * may have modified the values in the specified rows.  This does not actually remove
	 * the entity instances themselves from the instance cache.  Rather, it just flushes all
	 * of their internally cached values (with the exception of the primary key).
	 */
	public void flush(RawEntity<?>... entities) {
		List<Class<? extends RawEntity<?>>> types = new ArrayList<Class<? extends RawEntity<?>>>(entities.length);
		Map<RawEntity<?>, EntityProxy<?, ?>> toFlush = new HashMap<RawEntity<?>, EntityProxy<?, ?>>();

		proxyLock.readLock().lock();
		try {
			for (RawEntity<?> entity : entities) {
				verify(entity);

				types.add(entity.getEntityType());
				toFlush.put(entity, proxies.get(entity));
			}
		} finally {
			proxyLock.readLock().unlock();
		}

		for (Entry<RawEntity<?>, EntityProxy<?, ?>> entry : toFlush.entrySet()) {
			entry.getValue().flushCache(entry.getKey());
		}

		relationsCache.remove(types.toArray(new Class[types.size()]));
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
	public <T extends RawEntity<K>, K> T[] get(final Class<T> type, K... keys) {
		final String primaryKeyField = Common.getPrimaryKeyField(type, getFieldNameConverter());
		final String tableName = getTableNameConverter().getName(type);

		return getFromCache(type, new Function<T, K>() {
			public T invoke(K key) {
				T back = null;
				Connection conn = null;

				try {
					conn = provider.getConnection();

					StringBuilder sql = new StringBuilder("SELECT ");
					sql.append(provider.processID(primaryKeyField));
					sql.append(" FROM ").append(provider.processID(tableName));
					sql.append(" WHERE ").append(provider.processID(primaryKeyField));
					sql.append(" = ?");

					PreparedStatement stmt = conn.prepareStatement(sql.toString());

					DatabaseType<K> dbType = (DatabaseType<K>) TypeManager.getInstance().getType(key.getClass());
					dbType.putToDatabase(EntityManager.this, stmt, 1, key);

					ResultSet res = stmt.executeQuery();
					if (res.next()) {
						back = getAndInstantiate(type, key);
					}

					res.close();
					stmt.close();
				} catch (SQLException e) {
				} finally {
					if (conn != null) {
						try {
							conn.close();
						} catch (SQLException e) {
						}
					}
				}

				return back;
			}
		}, keys);
	}

	protected <T extends RawEntity<K>, K> T[] peer(final Class<T> type, K... keys) {
		return getFromCache(type, new Function<T, K>() {
			public T invoke(K key) {
				return getAndInstantiate(type, key);
			}
		}, keys);
	}

	private <T extends RawEntity<K>, K> T[] getFromCache(Class<T> type, Function<T, K> create, K... keys) {
		T[] back = (T[]) Array.newInstance(type, keys.length);
		int index = 0;

		for (K key : keys) {
			entityCacheLock.writeLock().lock();
			try {	// upcast to workaround bug in javac
				Reference<?> reference = entityCache.get(new CacheKey<K>(key, type));
				Reference<T> ref = (Reference<T>) reference;
				T entity = (ref == null ? null : ref.get());

				if (entity != null) {
					back[index++] = entity;
				} else {
					back[index++] = create.invoke(key);
				}
			} finally {
				entityCacheLock.writeLock().unlock();
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

		proxyLock.writeLock().lock();
		try {
			proxies.put(entity, proxy);
		} finally {
			proxyLock.writeLock().unlock();
		}

		entityCache.put(new CacheKey<K>(key, type), createRef(entity));
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
	public <T extends RawEntity<K>, K> T get(Class<T> type, K key) {
        return get(type, toArray(key))[0];
	}

    protected <T extends RawEntity<K>, K> T peer(Class<T> type, K key) {
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
	 * {@link DatabaseProvider#insertReturningKey(EntityManager, Connection, Class, String, boolean, String, DBParam...)}.
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
	 * @see net.java.ao.DatabaseProvider#insertReturningKey(EntityManager, Connection, Class, String, boolean, String, DBParam...)
	 */
	public <T extends RawEntity<K>, K> T create(Class<T> type, DBParam... params) throws SQLException {
		T back = null;
		String table = tableNameConverter.getName(type);

		Set<DBParam> listParams = new HashSet<DBParam>();
		listParams.addAll(Arrays.asList(params));

        for (Method method : MethodFinder.getInstance().findAnnotatedMethods(Generator.class, type)) {
            Generator genAnno = method.getAnnotation(Generator.class);
            String field = fieldNameConverter.getName(method);
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

		final Connection conn = provider.getConnection();
		try {
			Method pkMethod = Common.getPrimaryKeyMethod(type);
			back = peer(type, provider.insertReturningKey(this, conn,
					Common.getPrimaryKeyClassType(type),
					Common.getPrimaryKeyField(type, getFieldNameConverter()),
					pkMethod.getAnnotation(AutoIncrement.class) != null, table, listParams.toArray(new DBParam[listParams.size()])));
		} finally {
			conn.close();
		}

		relationsCache.remove(type);

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

		entityCacheLock.writeLock().lock();
		try {
			final Connection conn = provider.getConnection();
			try {
				for (Class<? extends RawEntity<?>> type : organizedEntities.keySet()) {
					List<RawEntity<?>> entityList = organizedEntities.get(type);

					StringBuilder sql = new StringBuilder("DELETE FROM ");
                    sql.append(provider.processID(tableNameConverter.getName(type)));
					sql.append(" WHERE ").append(provider.processID(
							Common.getPrimaryKeyField(type, getFieldNameConverter()))).append(" IN (?");

					for (int i = 1; i < entityList.size(); i++) {
						sql.append(",?");
					}
					sql.append(')');

					final PreparedStatement stmt = provider.preparedStatement(conn, sql);

					int index = 1;
					for (RawEntity<?> entity : entityList) {
						TypeManager.getInstance().getType((Class) entity.getEntityType()).putToDatabase(this, stmt, index++, entity);
					}

					relationsCache.remove(type);
					stmt.executeUpdate();
					stmt.close();
				}
			} finally {
				conn.close();
			}

			for (RawEntity<?> entity : entities) {
				entityCache.remove(new CacheKey(Common.getPrimaryKeyValue(entity), entity.getEntityType()));
			}

			proxyLock.writeLock().lock();
			try {
				for (RawEntity<?> entity : entities) {
					proxies.remove(entity);
				}
			} finally {
				proxyLock.writeLock().unlock();
			}
		} finally {
			entityCacheLock.writeLock().unlock();
		}
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
	public <T extends RawEntity<K>, K> T[] find(Class<T> type, String criteria, Object... parameters) throws SQLException {
		return find(type, Query.select().where(criteria, parameters));
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
	public <T extends RawEntity<K>, K> T[] find(Class<T> type, Query query) throws SQLException {
		String selectField = Common.getPrimaryKeyField(type, getFieldNameConverter());
		query.resolveFields(type, getFieldNameConverter());

		String[] fields = query.getFields();
		if (fields.length == 1) {
			selectField = fields[0];
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
	public <T extends RawEntity<K>, K> T[] find(Class<T> type, String field, Query query) throws SQLException {
		List<T> back = new ArrayList<T>();

		query.resolveFields(type, getFieldNameConverter());

		final Preload preloadAnnotation = type.getAnnotation(Preload.class);
		if (preloadAnnotation != null) {
			if (!query.getFields()[0].equals("*") && query.getJoins().isEmpty()) {
				String[] oldFields = query.getFields();
				List<String> newFields = new ArrayList<String>();

				for (String newField : preloadValue(preloadAnnotation, fieldNameConverter)) {
					newField = newField.trim();

					int fieldLoc = -1;
					for (int i = 0; i < oldFields.length; i++) {
						if (oldFields[i].equals(newField)) {
							fieldLoc = i;
							break;
						}
					}

					if (fieldLoc < 0) {
						newFields.add(newField);
					} else {
						newFields.add(oldFields[fieldLoc]);
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

		final Connection conn = provider.getConnection();
		try {
			final String sql = query.toSQL(type, provider, tableNameConverter, getFieldNameConverter(), false);

			final PreparedStatement stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			provider.setQueryStatementProperties(stmt, query);

            query.setParameters(this, stmt);

            ResultSet res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);

            final DatabaseType<K> primaryKeyType = Common.getPrimaryKeyType(type);
            final Class<K> primaryKeyClassType = Common.getPrimaryKeyClassType(type);
            final String[] canonicalFields = query.getCanonicalFields(type, fieldNameConverter);
            while (res.next())
            {
                final T entity = peer(type, primaryKeyType.pullFromDatabase(this, res, primaryKeyClassType, field));
                final CacheLayer cacheLayer = getProxyForEntity(entity).getCacheLayer(entity);

                for (String cacheField : canonicalFields)
                {
                    cacheLayer.put(cacheField, res.getObject(cacheField));
                }

                back.add(entity);
            }
            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
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

		final Connection conn = provider.getConnection();
		try {
			final PreparedStatement stmt = provider.preparedStatement(conn, sql);

			TypeManager manager = TypeManager.getInstance();
			for (int i = 0; i < parameters.length; i++) {
				Class javaType = parameters[i].getClass();

				if (parameters[i] instanceof RawEntity) {
					javaType = ((RawEntity<?>) parameters[i]).getEntityType();
				}

				manager.getType(javaType).putToDatabase(this, stmt, i + 1, parameters[i]);
			}

			ResultSet res = stmt.executeQuery();
			while (res.next()) {
				back.add(peer(type, Common.getPrimaryKeyType(type).pullFromDatabase(this, res, (Class<? extends K>) type, keyField)));
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
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
	    stream(type, Query.select(), streamCallback);
	}
	
    /**
     * <p>Selects all entities of the given type and feeds them to the callback, one by one. The entities are slim, uncached, read-only
     * representations of the data. They only supports getters or designated {@link Accessor} methods. Calling setters or <pre>save</pre> will 
     * result in an exception. Other method calls will be ignored. The proxies do not support lazy-loading of related entities.</p>
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
        // select all fields, as lazy loading would be too expensive
        query.setFields(new String[]{"*"});

        // fetch some information about the fields we're dealing with. These calls are expensive when
        // executed too often, and since we're always working on the same type of object, we only need them once.
        final DatabaseType<K> primaryKeyType = Common.getPrimaryKeyType(type);
        final Class<K> primaryKeyClassType = Common.getPrimaryKeyClassType(type);
        final String[] canonicalFields = query.getCanonicalFields(type, fieldNameConverter);
        String field = Common.getPrimaryKeyField(type, getFieldNameConverter());

        // the factory caches details about the proxied interface, since reflection calls are expensive
        // inside the stream loop
        ReadOnlyEntityProxyFactory<T, K> proxyFactory = new ReadOnlyEntityProxyFactory<T, K>(this, type);
        
        // Execute the query
        final Connection conn = provider.getConnection();
        try {
            final String sql = query.toSQL(type, provider, tableNameConverter, getFieldNameConverter(), false);
            
            // we're only going over the result set once, so use the slimmest possible cursor type
            final PreparedStatement stmt = provider.preparedStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            provider.setQueryStatementProperties(stmt, query);
            
            query.setParameters(this, stmt);
            
            ResultSet res = stmt.executeQuery();
            provider.setQueryResultSetProperties(res, query);
            
            while (res.next())
            {
                K primaryKey = primaryKeyType.pullFromDatabase(this, res, primaryKeyClassType, field);
                // use the cached instance information from the factory to build efficient, read-only proxy representations
                ReadOnlyEntityProxy<T, K> proxy = proxyFactory.build(primaryKey);
                T entity = type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type, EntityProxyAccessor.class}, proxy));
                
                // transfer the values from the result set into the local proxy cache. We're not caching the proxy itself anywhere, since
                // it's designated as a read-only snapshot view of the data and thus doesn't need flushing.
                for (String fieldName : canonicalFields)
                {
                    proxy.addValue(fieldName, res.getObject(fieldName));
                }

                // forward the proxy to the callback for the client to consume
                streamCallback.onRowRead(entity);
            }
            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
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
	public <K> int count(Class<? extends RawEntity<K>> type, Query query) throws SQLException {
		int back = -1;

        Connection conn = provider.getConnection();
		try {
			String sql = query.toSQL(type, provider, tableNameConverter, getFieldNameConverter(), true);

			final PreparedStatement stmt = provider.preparedStatement(conn, sql);
			provider.setQueryStatementProperties(stmt, query);

			query.setParameters(this, stmt);

			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				back = res.getInt(1);
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}

		return back;
	}

	/**
	 * Retrieves the {@link TableNameConverter} instance used for name
	 * conversion of all entity types.
	 */
	public TableNameConverter getTableNameConverter() {
        return tableNameConverter;
	}

	/**
	 * Retrieves the {@link FieldNameConverter} instance used for name
	 * conversion of all entity methods.
	 */
	public FieldNameConverter getFieldNameConverter() {
        return fieldNameConverter;
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
		proxyLock.readLock().lock();
		try {
            return ((EntityProxyAccessor) entity).getEntityProxy();
		} finally {
			proxyLock.readLock().unlock();
		}
	}

	RelationsCache getRelationsCache() {
		return relationsCache;
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

	private static interface Function<R, F> {
		public R invoke(F formals);
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
