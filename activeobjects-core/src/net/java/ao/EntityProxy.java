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

import com.google.common.base.Defaults;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.info.FieldInfo;
import net.java.ao.schema.info.EntityInfo;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static net.java.ao.Common.getAttributeTypeFromMethod;
import static net.java.ao.Common.preloadValue;
import static net.java.ao.Common.where;
import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 * @author Daniel Spiewak
 */
public class EntityProxy<T extends RawEntity<K>, K> implements InvocationHandler
{
	private final K key;
	private final EntityInfo<T, K> entityInfo;

	private final EntityManager manager;

    private final Map<String, Object> values = new HashMap<String, Object>();
    private final Set<String> dirty = new HashSet<String>();
    private final Lock lockValuesDirty = new ReentrantLock();

	private ImplementationWrapper<T> implementation;
	private List<PropertyChangeListener> listeners;

	EntityProxy(EntityManager manager, EntityInfo<T, K> entityInfo, K key) {
		this.key = key;
		this.entityInfo = entityInfo;
		this.manager = manager;
		
		listeners = new LinkedList<PropertyChangeListener>();
	}

    private FieldNameConverter getFieldNameConverter()
    {
        return manager.getNameConverters().getFieldNameConverter();
    }

    @SuppressWarnings("unchecked")
	public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        final String methodName = method.getName();

        if(methodName.equals("getEntityProxy")) {
            return this;
        }

		if (methodName.equals("getEntityType")) {
			return getType();
		}

		if (implementation == null) {
			implementation = new ImplementationWrapper<T>();
			implementation.init((T) proxy);
		}

		final MethodImplWrapper methodImpl = implementation.getMethod(methodName, method.getParameterTypes());
		if (methodImpl != null) {
			final Class<?> declaringClass = methodImpl.getMethod().getDeclaringClass();
			if (!Object.class.equals(declaringClass)) {
                // We don't want to return get the class Class using Class.forName as this doesn't play well
                // with multiple ClassLoaders (if the AO library has a separate class loader to the AO client)
                // Instead just compare the classNames
				final String callingClassName = Common.getCallingClassName(1);
				if (callingClassName == null || !callingClassName.equals(declaringClass.getName())) {
					return methodImpl.getMethod().invoke(methodImpl.getInstance(), args);
				}
			}
		}

		if (entityInfo.hasAccessor(method) && entityInfo.getField(method).equals(entityInfo.getPrimaryKey())) {
			return getKey();
		}
        if (methodName.equals("save")) {
			save((RawEntity<K>) proxy);
			return Void.TYPE;
		}
        if (methodName.equals("getEntityManager")) {
            return manager;
        }
        if (methodName.equals("addPropertyChangeListener")) {
			addPropertyChangeListener((PropertyChangeListener) args[0]);
			return null;
		}
        if (methodName.equals("removePropertyChangeListener")) {
			removePropertyChangeListener((PropertyChangeListener) args[0]);
			return null;
		}
        if (methodName.equals("hashCode")) {
			return hashCodeImpl();
		}
        if (methodName.equals("equals")) {
			return equalsImpl((RawEntity<K>) proxy, args[0]);
		}
        if (methodName.equals("toString")) {
			return toStringImpl();
		}
        if (methodName.equals("init")) {
			return null;
		}

        final FieldInfo fieldInfo = entityInfo.getField(method);

        if (fieldInfo != null && !fieldInfo.isNullable() && args != null && args.length > 0) {
            if (args[0] == null) {
                throw new IllegalArgumentException("Field '" + getFieldNameConverter().getName(method) + "' does not accept null values");
            }
        }

        // TODO move this information into EntityInfo and friends
        final OneToOne oneToOneAnnotation = method.getAnnotation(OneToOne.class);
        final boolean isOneToOne = oneToOneAnnotation != null && RawEntity.class.isAssignableFrom(method.getReturnType());
        final OneToMany oneToManyAnnotation = method.getAnnotation(OneToMany.class);
        final boolean isOneToMany = oneToManyAnnotation != null && method.getReturnType().isArray() && RawEntity.class.isAssignableFrom(method.getReturnType().getComponentType());
        final ManyToMany manyToManyAnnotation = method.getAnnotation(ManyToMany.class);
        final boolean isManyToMany = manyToManyAnnotation != null && method.getReturnType().isArray() && RawEntity.class.isAssignableFrom(method.getReturnType().getComponentType());
        if (isOneToOne || isOneToMany || isManyToMany) {
            final Object ret;
            lockValuesDirty.lock();
            try {
                if (values.containsKey(methodName)) {
                    ret = values.get(methodName);
                } else if (isOneToOne) {
                    if (oneToOneAnnotation.reverse().isEmpty()) {
                        ret = legacyFetchOneToOne((RawEntity<K>) proxy, method, oneToOneAnnotation);
                    } else {
                        ret = fetchOneToOne(method, oneToOneAnnotation);
                    }
                    values.put(methodName, ret);
                } else if (isOneToMany) {
                    if (oneToManyAnnotation.reverse().isEmpty()) {
                        ret = legacyFetchOneToMany((RawEntity<K>) proxy, method, oneToManyAnnotation);
                    } else {
                        ret = fetchOneToMany(method, oneToManyAnnotation);
                    }
                    values.put(methodName, ret);
                } else if (isManyToMany) {
                    if (manyToManyAnnotation.reverse().isEmpty() || manyToManyAnnotation.through().isEmpty()) {
                        ret = legacyFetchManyToMany((RawEntity<K>) proxy, method, manyToManyAnnotation);
                    } else {
                        ret = fetchManyToMany(method, manyToManyAnnotation);
                    }
                    values.put(methodName, ret);
                } else {
                    ret = null;
                }
            } finally {
                lockValuesDirty.unlock();
            }
            return ret;
        }

        if (fieldInfo != null) {

            if (method.equals(fieldInfo.getAccessor())) {
                return invokeGetter(fieldInfo);
            }

            if (method.equals(fieldInfo.getMutator())) {
                invokeSetter(getFieldNameConverter().getName(method), args[0], fieldInfo.getPolymorphicName());
                return Void.TYPE;
            }
        }

		throw new IllegalArgumentException("Cannot handle method. It is not a valid getter or setter and does not have an implementation supplied. " +
                "Signature: " + method.toString());
	}

    private RawEntity[] fetchManyToMany(final Method method, final ManyToMany annotation) throws SQLException, NoSuchMethodException
    {
        final Class remoteType = method.getReturnType().getComponentType();
        final Class throughType = annotation.value();
        final String whereClause = Common.where(annotation, getFieldNameConverter());
        final Preload preloadAnnotation = (Preload) remoteType.getAnnotation(Preload.class);
        final Method reverseMethod = throughType.getMethod(annotation.reverse());
        final Method throughMethod = throughType.getMethod(annotation.through());
        final String reversePolymorphicTypeFieldName = getAttributeTypeFromMethod(reverseMethod).isAnnotationPresent(Polymorphic.class) ? getFieldNameConverter().getPolyTypeName(reverseMethod) : null;
        final String remotePolymorphicTypeFieldName = getAttributeTypeFromMethod(throughMethod).isAnnotationPresent(Polymorphic.class) ? getFieldNameConverter().getPolyTypeName(throughMethod) : null;
        final String returnField = getFieldNameConverter().getName(throughMethod);
        final Set<String> selectFields = new LinkedHashSet<String>();
        final DatabaseProvider provider = manager.getProvider();
        final StringBuilder sql = new StringBuilder("SELECT t.").append(provider.processID(returnField));

        if (remotePolymorphicTypeFieldName != null) {
            sql.append(", t.").append(provider.processID(remotePolymorphicTypeFieldName));
        } else {
            if (preloadAnnotation != null) {
                selectFields.addAll(preloadValue(preloadAnnotation, getFieldNameConverter()));
            } else {
                selectFields.addAll(Common.getValueFieldsNames(manager.resolveEntityInfo(remoteType), getFieldNameConverter()));
            }
            if (selectFields.contains(Preload.ALL)) {
                sql.append(", r.*");
            } else {
                for (final String field : selectFields) {
                    sql.append(", r.").append(manager.getProvider().processID(field));
                }
            }
        }

        final String throughTable = provider.withSchema(getTableNameConverter().getName(throughType));
        sql.append(" FROM ").append(throughTable).append(" t ");
        final String remotePrimaryKeyField = Common.getPrimaryKeyField(remoteType, getFieldNameConverter());
        if (!selectFields.isEmpty()) {
            final String remoteTable = provider.withSchema(getTableNameConverter().getName(remoteType));
            sql.append(" INNER JOIN ").append(remoteTable).append(" r ON t.").append(provider.processID(returnField)).append(" = r.").append(provider.processID(remotePrimaryKeyField));
        }
        final String reverseField = provider.processID(getFieldNameConverter().getName(reverseMethod));
        sql.append(" WHERE ");
        if (!selectFields.isEmpty()) {
            sql.append("t.");
        }
        sql.append(reverseField).append(" = ?");
        if (reversePolymorphicTypeFieldName != null)
        {
            sql.append(" AND ");
            if (!selectFields.isEmpty()) {
                sql.append("t.");
            }
            sql.append(provider.processID(reversePolymorphicTypeFieldName)).append(" = ?");
        }
        if (!whereClause.trim().equals(""))
        {
            sql.append(" AND (").append(provider.processWhereClause(whereClause)).append(")");
        }
        final List<RawEntity> back = new ArrayList<RawEntity>();
        final Connection conn = provider.getConnection();
        try
        {
            final PreparedStatement stmt = provider.preparedStatement(conn, sql);
            try
            {
                final TypeInfo<K> dbType = getTypeManager().getType(getClass(key));
                dbType.getLogicalType().putToDatabase(manager, stmt, 1, key, dbType.getJdbcWriteType());
                if (reversePolymorphicTypeFieldName != null)
                {
                    stmt.setString(2, manager.getPolymorphicTypeMapper().convert(entityInfo.getEntityType()));
                }
                final TypeInfo<K> primaryKeyType = Common.getPrimaryKeyType(provider.getTypeManager(), (Class<? extends RawEntity<K>>) remoteType);
                final ResultSet res = stmt.executeQuery();
                try
                {
                    while (res.next())
                    {
                        EntityInfo entityInfo = manager.resolveEntityInfo((remotePolymorphicTypeFieldName == null ? remoteType : manager.getPolymorphicTypeMapper().invert(remoteType, res.getString(remotePolymorphicTypeFieldName))));
                        if (selectFields.remove(Preload.ALL))
                        {
                            selectFields.addAll(entityInfo.getFieldNames());
                        }
                        final RawEntity returnValueEntity = manager.peer(entityInfo, primaryKeyType.getLogicalType().pullFromDatabase(manager, res, throughType, returnField));
                        final EntityProxy<?, ?> proxy = manager.getProxyForEntity(returnValueEntity);
                        proxy.lockValuesDirty.lock();
                        try
                        {
                            for (final String field : selectFields)
                            {
                                proxy.values.put(field, res.getObject(field));
                            }
                        }
                        finally
                        {
                            proxy.lockValuesDirty.unlock();
                        }
                        back.add(returnValueEntity);
                    }
                }
                finally
                {
                    res.close();
                }
            }
            finally
            {
                stmt.close();
            }
        }
        finally
        {
            conn.close();
        }
        return back.toArray((RawEntity[]) Array.newInstance(remoteType, back.size()));
    }

    private RawEntity[] fetchOneToMany(final Method method, final OneToMany annotation) throws SQLException, NoSuchMethodException
    {
        final Class remoteType = method.getReturnType().getComponentType();
        final EntityInfo entityInfo = manager.resolveEntityInfo(remoteType);
        final String remotePrimaryKeyFieldName = Common.getPrimaryKeyField(remoteType, getFieldNameConverter());
        final String whereClause = where(annotation, getFieldNameConverter());
        final Preload preloadAnnotation = (Preload) remoteType.getAnnotation(Preload.class);
        final Method remoteMethod = remoteType.getMethod(annotation.reverse());
        final String remotePolymorphicTypeFieldName = getPolymorphicTypeFieldName(remoteMethod);
        final StringBuilder sql = new StringBuilder("SELECT ");
        final Set<String> selectFields = new LinkedHashSet<String>();

        selectFields.add(remotePrimaryKeyFieldName);
        if (remotePolymorphicTypeFieldName == null) {
            if (preloadAnnotation != null) {
                selectFields.addAll(preloadValue(preloadAnnotation, getFieldNameConverter()));
            } else {
                selectFields.addAll(Common.getValueFieldsNames(entityInfo, getFieldNameConverter()));
            }
        }
        if (selectFields.contains(Preload.ALL)) {
            sql.append(Preload.ALL);
        } else {
            for (final String field : selectFields) {
                sql.append(manager.getProvider().processID(field)).append(',');
            }
            sql.setLength(sql.length() - 1);
        }

        sql.append(" FROM ").append(manager.getProvider().withSchema(getTableNameConverter().getName(remoteType)));
        sql.append(" WHERE ").append(manager.getProvider().processID(getFieldNameConverter().getName(remoteMethod))).append(" = ?");
        if (!whereClause.trim().equals("")) {
            sql.append(" AND (").append(manager.getProvider().processWhereClause(whereClause)).append(")");
        }
        if (remotePolymorphicTypeFieldName != null) {
            sql.append(" AND ").append(manager.getProvider().processID(remotePolymorphicTypeFieldName)).append(" = ?");
        }
        final Connection conn = manager.getProvider().getConnection();
        try {
            final PreparedStatement stmt = manager.getProvider().preparedStatement(conn, sql);
            try
            {
                final TypeInfo<K> dbType = getTypeManager().getType(getClass(key));
                dbType.getLogicalType().putToDatabase(manager, stmt, 1, key, dbType.getJdbcWriteType());
                if (remotePolymorphicTypeFieldName != null) {
                    stmt.setString(2, manager.getPolymorphicTypeMapper().convert(entityInfo.getEntityType()));
                }
                final ResultSet res = stmt.executeQuery();
                try {
                    final List<RawEntity<?>> result = new ArrayList<RawEntity<?>>();
                    while (res.next()) {
                        final Object returnValue = Common.getPrimaryKeyType(getTypeManager(), (Class) remoteType).getLogicalType().pullFromDatabase(manager, res, (Class) remoteType, remotePrimaryKeyFieldName);
                        final RawEntity<?> returnValueEntity = manager.peer(entityInfo, returnValue);
                        final EntityProxy<?, ?> proxy = manager.getProxyForEntity(returnValueEntity);
                        if (selectFields.remove(Preload.ALL))
                        {
                            selectFields.addAll(entityInfo.getFieldNames());
                        }
                        proxy.lockValuesDirty.lock();
                        try {
                            for (final String field : selectFields) {
                                proxy.values.put(field, res.getObject(field));
                            }
                        } finally {
                            proxy.lockValuesDirty.unlock();
                        }
                        result.add(returnValueEntity);
                    }
                    return result.toArray((RawEntity<?>[]) Array.newInstance(remoteType, result.size()));
                } finally {
                    res.close();
                }
            } finally {
                stmt.close();
            }
        } finally {
            conn.close();
        }
    }

    private String getPolymorphicTypeFieldName(final Method remoteMethod)
    {
        final Class<?> attributeType = getAttributeTypeFromMethod(remoteMethod);
        return attributeType != null && attributeType.isAssignableFrom(getType())
                && attributeType.isAnnotationPresent(Polymorphic.class) ? getFieldNameConverter().getPolyTypeName(remoteMethod) : null;
    }

    private RawEntity fetchOneToOne(final Method method, final OneToOne annotation) throws SQLException, NoSuchMethodException
    {
        final Class remoteType = method.getReturnType();
        final EntityInfo entityInfo = manager.resolveEntityInfo(remoteType);
        final String remotePrimaryKeyFieldName = Common.getPrimaryKeyField(remoteType, getFieldNameConverter());
        final String whereClause = Common.where(annotation, getFieldNameConverter());
        final Method remoteMethod = remoteType.getMethod(annotation.reverse());
        final String remotePolymorphicTypeFieldName = getPolymorphicTypeFieldName(remoteMethod);
        final Preload preloadAnnotation = (Preload) remoteType.getAnnotation(Preload.class);
        final StringBuilder sql = new StringBuilder("SELECT ");
        final Set<String> selectFields = new LinkedHashSet<String>();

        selectFields.add(remotePrimaryKeyFieldName);
        if (remotePolymorphicTypeFieldName == null) {
            if (preloadAnnotation != null) {
                selectFields.addAll(preloadValue(preloadAnnotation, getFieldNameConverter()));
            } else {
                selectFields.addAll(Common.getValueFieldsNames(entityInfo, getFieldNameConverter()));
            }
        }
        if (selectFields.contains(Preload.ALL)) {
            sql.append(Preload.ALL);
        } else {
            for (final String field : selectFields) {
                sql.append(manager.getProvider().processID(field)).append(',');
            }
            sql.setLength(sql.length() - 1);
        }

        sql.append(" FROM ").append(manager.getProvider().withSchema(getTableNameConverter().getName(remoteType)));
        sql.append(" WHERE ").append(manager.getProvider().processID(getFieldNameConverter().getName(remoteMethod))).append(" = ?");
        if (whereClause.trim().length() != 0)
        {
            sql.append(" AND (").append(manager.getProvider().processWhereClause(whereClause)).append(")");
        }
        if (remotePolymorphicTypeFieldName != null)
        {
            sql.append(" AND ").append(manager.getProvider().processID(remotePolymorphicTypeFieldName)).append(" = ?");
        }
        final Connection conn = manager.getProvider().getConnection();
        try
        {
            final PreparedStatement stmt = manager.getProvider().preparedStatement(conn, sql);
            try
            {
                final TypeInfo<K> dbType = getTypeManager().getType(getClass(key));
                dbType.getLogicalType().putToDatabase(manager, stmt, 1, key, dbType.getJdbcWriteType());
                if (remotePolymorphicTypeFieldName != null)
                {
                    stmt.setString(2, manager.getPolymorphicTypeMapper().convert(entityInfo.getEntityType()));
                }
                final ResultSet res = stmt.executeQuery();
                try
                {
                    if (res.next())
                    {
                        final RawEntity returnValueEntity = manager.peer(entityInfo, Common.getPrimaryKeyType(getTypeManager(), (Class<? extends RawEntity<K>>) remoteType).getLogicalType().pullFromDatabase(manager, res, (Class<K>) remoteType, remotePrimaryKeyFieldName));
                        if (selectFields.remove(Preload.ALL))
                        {
                            selectFields.addAll(entityInfo.getFieldNames());
                        }
                        final EntityProxy<?, ?> proxy = manager.getProxyForEntity(returnValueEntity);
                        proxy.lockValuesDirty.lock();
                        try
                        {
                            for (final String field : selectFields)
                            {
                                proxy.values.put(field, res.getObject(field));
                            }
                        }
                        finally
                        {
                            proxy.lockValuesDirty.unlock();
                        }
                        return returnValueEntity;
                    }
                    else
                    {
                        return null;
                    }
                }
                finally
                {
                    res.close();
                }
            }
            finally
            {
                stmt.close();
            }
        }
        finally
        {
            conn.close();
        }
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    private RawEntity[] legacyFetchManyToMany(final RawEntity<K> proxy, final Method method, final ManyToMany manyToManyAnnotation) throws SQLException
    {
        final Class<? extends RawEntity<?>> throughType = manyToManyAnnotation.value();
        final Class<? extends RawEntity<?>> type = (Class<? extends RawEntity<?>>) method.getReturnType().getComponentType();
        return retrieveRelations(proxy, null,
                Common.getMappingFields(getFieldNameConverter(),
                        throughType, type), throughType, (Class<? extends RawEntity>) type,
                        Common.where(manyToManyAnnotation, getFieldNameConverter()),
                        Common.getPolymorphicFieldNames(getFieldNameConverter(), throughType, entityInfo.getEntityType()),
                        Common.getPolymorphicFieldNames(getFieldNameConverter(), throughType, type));
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    private RawEntity[] legacyFetchOneToMany(final RawEntity<K> proxy, final Method method, final OneToMany oneToManyAnnotation) throws SQLException
    {
        final Class<? extends RawEntity<?>> type = (Class<? extends RawEntity<?>>) method.getReturnType().getComponentType();
        return retrieveRelations(proxy, new String[0],
                new String[] { Common.getPrimaryKeyField(type, getFieldNameConverter()) },
                (Class<? extends RawEntity>) type, where(oneToManyAnnotation, getFieldNameConverter()),
                Common.getPolymorphicFieldNames(getFieldNameConverter(), type, entityInfo.getEntityType()));
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    private RawEntity legacyFetchOneToOne(final RawEntity<K> proxy, final Method method, final OneToOne oneToOneAnnotation) throws SQLException
    {
        Class<? extends RawEntity<?>> type = (Class<? extends RawEntity<?>>) method.getReturnType();
        final RawEntity[] back = retrieveRelations(proxy, new String[0],
                new String[] { Common.getPrimaryKeyField(type, getFieldNameConverter()) },
                (Class<? extends RawEntity>) type, Common.where(oneToOneAnnotation, getFieldNameConverter()),
                Common.getPolymorphicFieldNames(getFieldNameConverter(), type, entityInfo.getEntityType()));
        return back.length == 0 ? null : back[0];
    }

    private TableNameConverter getTableNameConverter()
    {
        return manager.getNameConverters().getTableNameConverter();
    }

    public K getKey() {
		return key;
	}

	@SuppressWarnings("unchecked")
	public void save(RawEntity entity) throws SQLException {
        lockValuesDirty.lock();
        try {
            if (dirty.isEmpty()) {
                return;
            }

            String table = entityInfo.getName();
            final DatabaseProvider provider = this.manager.getProvider();
            final TypeManager typeManager = provider.getTypeManager();
            Connection conn = null;
            PreparedStatement stmt = null;
            try
            {
                conn = provider.getConnection();
                StringBuilder sql = new StringBuilder("UPDATE " + provider.withSchema(table) + " SET ");

                for (String name : dirty) {
                    sql.append(provider.processID(name));

                    if (values.containsKey(name)) {
                        sql.append(" = ?,");
                    } else {
                        sql.append(" = NULL,");
                    }
                }

                if (sql.charAt(sql.length() - 1) == ',') {
                    sql.setLength(sql.length() - 1);
                }

                sql.append(" WHERE ").append(provider.processID(entityInfo.getPrimaryKey().getName())).append(" = ?");

                stmt = provider.preparedStatement(conn, sql);

                List<PropertyChangeEvent> events = new LinkedList<PropertyChangeEvent>();
                int index = 1;
                for (String name : dirty) {
                    if (!values.containsKey(name)) {
                        continue;
                    }

                    Object value = values.get(name);
                    FieldInfo fieldInfo = entityInfo.getField(name);

                    events.add(new PropertyChangeEvent(entity, name, null, value));

                    if (value == null) {
                        this.manager.getProvider().putNull(stmt, index++);
                    } else {
                        Class javaType = value.getClass();

                        if (value instanceof RawEntity) {
                            javaType = ((RawEntity) value).getEntityType();
                        }

                        TypeInfo dbType = typeManager.getType(javaType);
                        dbType.getLogicalType().validate(value);
                        dbType.getLogicalType().putToDatabase(this.manager, stmt, index++, value, dbType.getJdbcWriteType());

                        if (!fieldInfo.isStorable()) {
                            values.remove(name);
                        }
                    }
                }
                TypeInfo pkType = entityInfo.getPrimaryKey().getTypeInfo();
                pkType.getLogicalType().putToDatabase(this.manager, stmt, index, key, pkType.getJdbcWriteType());
                stmt.executeUpdate();
                dirty.clear();

                for (PropertyChangeListener l : listeners) {
                    for (PropertyChangeEvent evt : events) {
                        l.propertyChange(evt);
                    }
                }
            }
            finally
            {
                closeQuietly(stmt);
                closeQuietly(conn);
            }
        } finally {
            lockValuesDirty.unlock();
        }
    }

	public void addPropertyChangeListener(PropertyChangeListener listener) {
		listeners.add(listener);
	}

	public void removePropertyChangeListener(PropertyChangeListener listener) {
		listeners.remove(listener);
	}

	public int hashCodeImpl() {
		return (key.hashCode() + entityInfo.hashCode()) % (2 << 15);
	}

	public boolean equalsImpl(RawEntity<K> proxy, Object obj) {
		if (proxy == obj) {
			return true;
		}

		if (obj instanceof RawEntity<?>) {
			RawEntity<?> entity = (RawEntity<?>) obj;

            String ourTableName = getTableNameConverter().getName(proxy.getEntityType());
            String theirTableName = getTableNameConverter().getName(entity.getEntityType());

			return Common.getPrimaryKeyValue(entity).equals(key) && theirTableName.equals(ourTableName);
		}

		return false;
	}

	public String toStringImpl() {
        return entityInfo.getName() + " {" + entityInfo.getPrimaryKey().getName() + " = " + key.toString() + "}";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}

		if (obj instanceof EntityProxy<?, ?>) {
			EntityProxy<?, ?> proxy = (EntityProxy<?, ?>) obj;

			if (proxy.entityInfo.equals(entityInfo) && proxy.key.equals(key)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return hashCodeImpl();
	}

    /**
     * Thread safely update the values with those provided.
     * This is only used by {@link net.java.ao.EntityManager#find(Class, String, Query)}, which should really move
     * the leave the population of this class to itself.
     *
     * @param updatedValues mandatory
     */
    protected void updateValues(Map<String, Object> updatedValues) {
        lockValuesDirty.lock();
        try {
            for (Map.Entry<String, Object> updatedValue : updatedValues.entrySet()) {
                values.put(updatedValue.getKey(), updatedValue.getValue());
            }
        } finally {
            lockValuesDirty.unlock();
        }
    }

	Class<T> getType() {
		return entityInfo.getEntityType();
	}

    private <V> V invokeGetter(FieldInfo<V> fieldInfo) throws Throwable {
        final Class<V> type = fieldInfo.getJavaType();
        final String name = fieldInfo.getName();
        final boolean isStorable = fieldInfo.isStorable();

        lockValuesDirty.lock();
        try {
            if (values.containsKey(name) && isStorable) {
                Object value = values.get(name);
                if (instanceOf(value, type)) {
                    //noinspection unchecked
                    return handleNullReturn((V) value, type);
                } else if (isBigDecimal(value, type)) { // Oracle for example returns BigDecimal when we expect doubles
                    //noinspection unchecked
                    return (V) handleBigDecimal(value, type);
                } else if (RawEntity.class.isAssignableFrom(type)) {
                    //noinspection unchecked
                    EntityInfo<? extends RawEntity, Object> remoteEntityInfo = manager.resolveEntityInfo((Class<? extends RawEntity>) type);
                    if (instanceOf(value, remoteEntityInfo.getPrimaryKey().getJavaType())) {
                        value = manager.peer(remoteEntityInfo, value);

                        values.put(name, value);
                        //noinspection unchecked
                        return handleNullReturn((V) value, type);
                    }
                }
            }

            final V back = pullFromDatabase(fieldInfo);

            if (isStorable) {
                values.put(name, back);
            }

            return handleNullReturn(back, type);
        } finally {
            lockValuesDirty.unlock();
        }
    }

    private <V> V pullFromDatabase(FieldInfo<V> fieldInfo) throws SQLException {
        Class<V> type = fieldInfo.getJavaType();
        String name = fieldInfo.getName();
        final DatabaseProvider provider = manager.getProvider();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
        V back = null;
        try {
            conn = provider.getConnection();
            StringBuilder sql = new StringBuilder("SELECT ");

            sql.append(provider.processID(name));
            String polyName = fieldInfo.getPolymorphicName();
            if (polyName != null) {
                sql.append(',').append(provider.processID(polyName));
            }

            sql.append(" FROM ").append(provider.withSchema(entityInfo.getName())).append(" WHERE ");
            sql.append(provider.processID(entityInfo.getPrimaryKey().getName())).append(" = ?");

            stmt = provider.preparedStatement(conn, sql);
            TypeInfo<K> pkType = entityInfo.getPrimaryKey().getTypeInfo();
            pkType.getLogicalType().putToDatabase(manager, stmt, 1, getKey(), pkType.getJdbcWriteType());

            res = stmt.executeQuery();
            if (res.next()) {
                back = convertValue(res, provider.shorten(name), provider.shorten(polyName), type);
            }
        } finally {
            closeQuietly(res, stmt, conn);
        }
        return back;
    }

    private <V> V handleNullReturn(V back, Class<V> type) {
        return back != null ? back : Defaults.defaultValue(type);
	}

    private void invokeSetter(String name, Object value, String polyName) throws Throwable {
        lockValuesDirty.lock();
        try {
            values.put(name, value);
            dirty.add(name);

            if (polyName != null) {
                String strValue = null;

                if (value != null) {
                    strValue = manager.getPolymorphicTypeMapper().convert(((RawEntity<?>) value).getEntityType());
                }

                values.put(polyName, strValue);
                dirty.add(polyName);
            }
        } finally {
            lockValuesDirty.unlock();
        }
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    private <V extends RawEntity<K>> V[] retrieveRelations(RawEntity<K> entity, String[] inMapFields,
			String[] outMapFields, Class<V> type, String where, String[] thisPolyNames) throws SQLException {
		return retrieveRelations(entity, inMapFields, outMapFields, type, type, where, thisPolyNames, null);
	}

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    private <V extends RawEntity<K>> V[] retrieveRelations(final RawEntity<K> entity,
                                                           String[] inMapFields,
                                                           final String[] outMapFields,
                                                           final Class<? extends RawEntity<?>> type,
                                                           final Class<V> finalType,
                                                           final String where,
                                                           final String[] thisPolyNames,
                                                           final String[] thatPolyNames) throws SQLException
    {
		if (inMapFields == null || inMapFields.length == 0) {
            inMapFields = Common.getMappingFields(getFieldNameConverter(), type, entityInfo.getEntityType());
		}
		List<V> back = new ArrayList<V>();
		List<String> resPolyNames = new ArrayList<String>(thatPolyNames == null ? 0 : thatPolyNames.length);

        String table = getTableNameConverter().getName(type);
		boolean oneToMany = type.equals(finalType);
		final Preload preloadAnnotation = finalType.getAnnotation(Preload.class);

        final DatabaseProvider provider = manager.getProvider();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet res = null;
		try {
            conn = provider.getConnection();
			StringBuilder sql = new StringBuilder();
			String returnField;
			String throughField;
			int numParams = 0;
			
			Set<String> selectFields = new LinkedHashSet<String>();
			
			if (oneToMany && inMapFields.length == 1 && outMapFields.length == 1 && (thatPolyNames == null || thatPolyNames.length == 0)) {
                // one-to-one, one-to-many non polymorphic relation
				sql.append("SELECT ");
				
				selectFields.add(outMapFields[0]);
                if (preloadAnnotation != null) {
                    selectFields.addAll(preloadValue(preloadAnnotation, getFieldNameConverter()));
                } else {
                    selectFields.addAll(Common.getValueFieldsNames(manager.resolveEntityInfo(finalType), getFieldNameConverter()));
                }

				if (selectFields.contains(Preload.ALL)) {
					sql.append(Preload.ALL);
				} else {
					for (String field : selectFields) {
						sql.append(provider.processID(field)).append(',');
					}
					sql.setLength(sql.length() - 1);
				}
				
				sql.append(" FROM ").append(provider.withSchema(table));
				
				sql.append(" WHERE ").append(provider.processID(inMapFields[0])).append(" = ?");
				
				if (!where.trim().equals("")) {
                    sql.append(" AND (").append(manager.getProvider().processWhereClause(where)).append(")");
				}
				
				if (thisPolyNames != null) {
					for (String name : thisPolyNames) {
						sql.append(" AND ").append(provider.processID(name)).append(" = ?");
					}
				}
				
				numParams++;
				returnField = outMapFields[0];
			}
            else if (!oneToMany && inMapFields.length == 1 && outMapFields.length == 1 && (thatPolyNames == null || thatPolyNames.length == 0)) {
                // many-to-many non polymorphic relation
                final String finalTable = getTableNameConverter().getName(finalType);
                final String finalTableAlias = "f";
                final String tableAlias = "t";

                returnField = manager.getProvider().shorten(finalTable + "__aointernal__id");
                throughField = manager.getProvider().shorten(table + "__aointernal__id");

				sql.append("SELECT ");

                String finalPKField = Common.getPrimaryKeyField(finalType, getFieldNameConverter());

				selectFields.add(finalPKField);
                if (preloadAnnotation != null) {
				    selectFields.addAll(preloadValue(preloadAnnotation, getFieldNameConverter()));
                } else {
                    selectFields.addAll(Common.getValueFieldsNames(manager.resolveEntityInfo(finalType), getFieldNameConverter()));
                }

                if (selectFields.contains(Preload.ALL)) {
                    selectFields.remove(Preload.ALL);
                    selectFields.addAll(Common.getValueFieldsNames(manager.resolveEntityInfo(finalType), getFieldNameConverter()));
                }

                sql.append(finalTableAlias).append('.').append(provider.processID(finalPKField));
                sql.append(" AS ").append(provider.quote(returnField)).append(',');

                selectFields.remove(finalPKField);

                sql.append(tableAlias).append('.').append(provider.processID(Common.getPrimaryKeyField(type, getFieldNameConverter())));
				sql.append(" AS ").append(provider.quote(throughField)).append(',');

				for (String field : selectFields) {
					sql.append(finalTableAlias).append('.').append(provider.processID(field)).append(',');
				}
				sql.setLength(sql.length() - 1);

				sql.append(" FROM ").append(provider.withSchema(table)).append(" ").append(tableAlias).append(" INNER JOIN ");
				sql.append(provider.withSchema(finalTable)).append(" ").append(finalTableAlias).append(" ON ");
				sql.append(tableAlias).append('.').append(provider.processID(outMapFields[0]));
				sql.append(" = ").append(finalTableAlias).append('.').append(provider.processID(finalPKField));

				sql.append(" WHERE ").append(tableAlias).append('.').append(
						provider.processID(inMapFields[0])).append(" = ?");

				if (!where.trim().equals("")) {
                    sql.append(" AND (").append(manager.getProvider().processWhereClause(where)).append(")");
				}

				if (thisPolyNames != null) {
					for (String name : thisPolyNames) {
						sql.append(" AND ").append(provider.processID(name)).append(" = ?");
					}
				}

				numParams++;
			} else if (inMapFields.length == 1 && outMapFields.length == 1) {
                // one-to-one, one-to-many or many-to-many polymorphic relation
				sql.append("SELECT ").append(provider.processID(outMapFields[0]));
				selectFields.add(outMapFields[0]);

				if (!oneToMany) {
                    throughField = Common.getPrimaryKeyField(type, getFieldNameConverter());

					sql.append(',').append(provider.processID(throughField));
					selectFields.add(throughField);
				}

				if (thatPolyNames != null) {
					for (String name : thatPolyNames) {
						resPolyNames.add(name);
						sql.append(',').append(provider.processID(name));
						selectFields.add(name);
					}
				}

				sql.append(" FROM ").append(provider.withSchema(table));
				sql.append(" WHERE ").append(provider.processID(inMapFields[0])).append(" = ?");

				if (!where.trim().equals("")) {
                    sql.append(" AND (").append(manager.getProvider().processWhereClause(where)).append(")");
				}

				if (thisPolyNames != null) {
					for (String name : thisPolyNames) {
						sql.append(" AND ").append(provider.processID(name)).append(" = ?");
					}
				}

				numParams++;
				returnField = outMapFields[0];
			} else {
				sql.append("SELECT DISTINCT a.outMap AS outMap");
				selectFields.add("outMap");

				if (thatPolyNames != null) {
					for (String name : thatPolyNames) {
						resPolyNames.add(name);
						sql.append(',').append("a.").append(provider.processID(name)).append(" AS ").append(
								provider.processID(name));
						selectFields.add(name);
					}
				}

				sql.append(" FROM (");
				returnField = "outMap";

				for (String outMap : outMapFields) {
					for (String inMap : inMapFields) {
						sql.append("SELECT ");
						sql.append(provider.processID(outMap));
						sql.append(" AS outMap,");
						sql.append(provider.processID(inMap));
						sql.append(" AS inMap");

						if (thatPolyNames != null) {
							for (String name : thatPolyNames) {
								sql.append(',').append(provider.processID(name));
							}
						}

						if (thisPolyNames != null) {
							for (String name : thisPolyNames) {
								sql.append(',').append(provider.processID(name));
							}
						}

						sql.append(" FROM ").append(provider.withSchema(table));
						sql.append(" WHERE ");
						sql.append(provider.processID(inMap)).append(" = ?");

						if (!where.trim().equals("")) {
                            sql.append(" AND (").append(manager.getProvider().processWhereClause(where)).append(")");
						}

						sql.append(" UNION ");

						numParams++;
					}
				}

				sql.setLength(sql.length() - " UNION ".length());
				sql.append(") a");

				if (thatPolyNames != null) {
					if (thatPolyNames.length > 0) {
						sql.append(" WHERE (");
					}

					for (String name : thatPolyNames) {
						sql.append("a.").append(provider.processID(name)).append(" = ?").append(" OR ");
					}

					if (thatPolyNames.length > 0) {
						sql.setLength(sql.length() - " OR ".length());
						sql.append(')');
					}
				}

				if (thisPolyNames != null) {
					if (thisPolyNames.length > 0) {
						if (thatPolyNames == null) {
							sql.append(" WHERE (");
						} else {
							sql.append(" AND (");
						}
					}

					for (String name : thisPolyNames) {
						sql.append("a.").append(provider.processID(name)).append(" = ?").append(" OR ");
					}

					if (thisPolyNames.length > 0) {
						sql.setLength(sql.length() - " OR ".length());
						sql.append(')');
					}
				}
			}

			stmt = provider.preparedStatement(conn, sql);

            TypeInfo<K> dbType =  getTypeManager().getType(getClass(key));
			int index = 0;
			for (; index < numParams; index++) {
                dbType.getLogicalType().putToDatabase(manager, stmt, index + 1, key, dbType.getJdbcWriteType());
			}
			
			int newLength = numParams + (thisPolyNames == null ? 0 : thisPolyNames.length);
            String typeValue = manager.getPolymorphicTypeMapper().convert(entityInfo.getEntityType());
			for (; index < newLength; index++) {
				stmt.setString(index + 1, typeValue);
			}

			dbType = Common.getPrimaryKeyType(provider.getTypeManager(), finalType);
			res = stmt.executeQuery();
			while (res.next()) {
                K returnValue = dbType.getLogicalType().pullFromDatabase(manager, res, (Class<K>) type, returnField);
				Class<V> backType = finalType;
				
				for (String polyName : resPolyNames) {
					if ((typeValue = res.getString(polyName)) != null) {
                        backType = (Class<V>) manager.getPolymorphicTypeMapper().invert(finalType, typeValue);
						break;
					}
				}
				
				if (backType.equals(entityInfo.getEntityType()) && returnValue.equals(key)) {
					continue;
				}
                final V returnValueEntity = manager.peer(manager.resolveEntityInfo(backType), returnValue);
                final EntityProxy<?, ?> proxy = manager.getProxyForEntity(returnValueEntity);
                if (selectFields.contains(Preload.ALL)) {
                    selectFields.remove(Preload.ALL);
                    selectFields.addAll(Common.getValueFieldsNames(manager.resolveEntityInfo(finalType), getFieldNameConverter()));
                }
                proxy.lockValuesDirty.lock();
                try {
                    for (String field : selectFields) {
                        if (!resPolyNames.contains(field)) {
                            proxy.values.put(field, res.getObject(field));
                        }
                    }
                } finally {
                    proxy.lockValuesDirty.unlock();
                }

				back.add(returnValueEntity);
			}
		} finally {
            closeQuietly(res, stmt, conn);
        }
		return back.toArray((V[]) Array.newInstance(finalType, back.size()));
	}

    private TypeManager getTypeManager()
    {
        return manager.getProvider().getTypeManager();
    }

    /**
     * Gets the generic class of the given type
     * @param object the type for which to get the class
     * @return the generic class
     */
    @SuppressWarnings("unchecked")
    private static <O> Class<O> getClass(O object)
    {
        return (Class<O>) object.getClass();
    }

    private <V> V convertValue(ResultSet res, String field, String polyName, Class<V> type) throws SQLException
    {
        if (isNull(res, field))
        {
            return null;
        }
		
		if (polyName != null) {
			Class<? extends RawEntity<?>> entityType = (Class<? extends RawEntity<?>>) type;
            entityType = manager.getPolymorphicTypeMapper().invert(entityType, res.getString(polyName));
			
			type = (Class<V>) entityType;		// avoiding Java cast oddities with generics
		}

        final TypeInfo<V> databaseType = getTypeManager().getType(type);
		
		if (databaseType == null) {
			throw new RuntimeException("UnrecognizedType: " + type.toString());
		}

        return databaseType.getLogicalType().pullFromDatabase(this.manager, res, type, field);
	}

    private boolean isNull(ResultSet res, String field) throws SQLException
    {
        res.getObject(field);
        return res.wasNull();
    }

    private boolean instanceOf(Object value, Class<?> type) {
		if (value == null) {
			return true;
		}
		
		if (type.isPrimitive()) {
			if (type.equals(boolean.class)) {
				return instanceOf(value, Boolean.class);
			} else if (type.equals(char.class)) {
				return instanceOf(value, Character.class);
			} else if (type.equals(byte.class)) {
				return instanceOf(value, Byte.class);
			} else if (type.equals(short.class)) {
				return instanceOf(value, Short.class);
			} else if (type.equals(int.class)) {
				return instanceOf(value, Integer.class);
			} else if (type.equals(long.class)) {
				return instanceOf(value, Long.class);
			} else if (type.equals(float.class)) {
				return instanceOf(value, Float.class);
			} else if (type.equals(double.class)) {
				return instanceOf(value, Double.class);
			}
		} else {
			return type.isInstance(value);
		}

		return false;
	}

    /**
     * Some DB (Oracle) return BigDecimal for about any number
     */
    private boolean isBigDecimal(Object value, Class<?> type)
    {
        return value instanceof BigDecimal && (isInteger(type) || isLong(type) || isFloat(type) || isDouble(type));
    }

    private Object handleBigDecimal(Object value, Class<?> type)
    {
        final BigDecimal bd = (BigDecimal) value;
        if (isInteger(type))
        {
            return bd.intValue();
        }
        else if (isLong(type))
        {
            return bd.longValue();
        }
        else if (isFloat(type))
        {
            return bd.floatValue();
        }
        else if (isDouble(type))
        {
            return bd.doubleValue();
        }
        else
        {
            throw new RuntimeException("Could not resolve actual type for object :" + value + ", expected type is " + type);
        }
    }

    private boolean isDouble(Class<?> type)
    {
        return type.equals(double.class) || type.equals(Double.class);
    }

    private boolean isFloat(Class<?> type)
    {
        return type.equals(float.class) || type.equals(Float.class);
    }

    private boolean isLong(Class<?> type)
    {
        return type.equals(long.class) || type.equals(Long.class);
    }

    private boolean isInteger(Class<?> type)
    {
        return type.equals(int.class) || type.equals(Integer.class);
    }

}
