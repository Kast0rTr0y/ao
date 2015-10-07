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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.FieldNameProcessor;
import net.java.ao.schema.Ignore;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.info.EntityInfo;
import net.java.ao.schema.info.FieldInfo;
import net.java.ao.sql.SqlUtils;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import net.java.ao.util.StringUtils;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

/**
 * WARNING: <strong>Not</strong> part of the public API.  This class is public only
 * to allow its use within other packages in the ActiveObjects library.
 *
 * @author Daniel Spiewak
 */
public final class Common {

    public static <T extends RawEntity<K>, K> T createPeer(EntityManager manager, Class<T> type, K key) throws SQLException {
        return manager.peer(manager.resolveEntityInfo(type), key);
    }

    public static String convertSimpleClassName(String name) {
        String[] array = name.split("\\.");
        return array[array.length - 1];
    }

    public static String convertDowncaseName(String name) {
        StringBuilder back = new StringBuilder();

        back.append(Character.toLowerCase(name.charAt(0)));
        back.append(name.substring(1));

        return back.toString();
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    public static String[] getMappingFields(FieldNameConverter converter, Class<? extends RawEntity<?>> from, Class<? extends RawEntity<?>> to) {
        Set<String> back = new LinkedHashSet<String>();

        for (Method method : from.getMethods()) {
            Class<?> attributeType = getAttributeTypeFromMethod(method);

            if (attributeType == null) {
                continue;
            }

            if (to.isAssignableFrom(attributeType)) {
                back.add(converter.getName(method));
            } else if (attributeType.getAnnotation(Polymorphic.class) != null
                    && attributeType.isAssignableFrom(to)) {
                back.add(converter.getName(method));
            }
        }

        return back.toArray(new String[back.size()]);
    }

    /**
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Deprecated
    public static String[] getPolymorphicFieldNames(FieldNameConverter converter, Class<? extends RawEntity<?>> from,
                                                    Class<? extends RawEntity<?>> to) {
        Set<String> back = new LinkedHashSet<String>();

        for (Method method : from.getMethods()) {
            Class<?> attributeType = getAttributeTypeFromMethod(method);

            if (attributeType != null && attributeType.isAssignableFrom(to)
                    && attributeType.getAnnotation(Polymorphic.class) != null) {
                back.add(converter.getPolyTypeName(method));
            }
        }

        return back.toArray(new String[back.size()]);
    }

    /**
     * <b>Note</b>: this method leads to the creation and quick discard of
     * large numbers of {@link AnnotationDelegate} objects.  Need to
     * do some research to determine whether or not this is actually
     * a problem.
     *
     * @deprecated All annotation information should be resolved upfront using {@link net.java.ao.schema.info.EntityInfo}
     */
    @Deprecated
    public static AnnotationDelegate getAnnotationDelegate(FieldNameConverter converter, Method method) {
        return new AnnotationDelegate(method, findCounterpart(converter, method));
    }

    /**
     * Finds the corresponding method in an accessor/mutator pair based
     * on the given method (or <code>null</code> if no corresponding method).
     *
     * @param converter TODO
     */
    private static Method findCounterpart(FieldNameConverter converter, Method method) {
        return MethodFinder.getInstance().findCounterPartMethod(converter, method);
    }

    public static boolean isMutator(Method method) {
        return (isAnnotatedMutator(method) || isNamedAsSetter(method)) && isValidMutator(method);
    }

    public static boolean isAnnotatedMutator(Method method) {
        return method.isAnnotationPresent(Mutator.class);
    }

    private static boolean isNamedAsSetter(Method method) {
        return method.getName().startsWith("set");
    }

    private static boolean isValidMutator(Method method) {
        return method.getReturnType() == Void.TYPE && method.getParameterTypes().length == 1;
    }

    public static boolean isAccessor(Method method) {
        return (isAnnotatedAccessor(method) || isNamedAsGetter(method)) && isValidAccessor(method);
    }

    private static boolean isAnnotatedAccessor(Method method) {
        return method.isAnnotationPresent(Accessor.class);
    }

    private static boolean isNamedAsGetter(Method method) {
        return method.getName().startsWith("get")
                || method.getName().startsWith("is");
    }

    private static boolean isValidAccessor(Method method) {
        return method.getReturnType() != Void.TYPE && method.getParameterTypes().length == 0;
    }

    public static boolean isAnnotatedAsRelational(Method method) {
        return method.isAnnotationPresent(OneToOne.class)
                || method.isAnnotationPresent(OneToMany.class)
                || method.isAnnotationPresent(ManyToMany.class);
    }

    public static Class<?> getAttributeTypeFromMethod(Method method) {
        if (isAnnotatedAsRelational(method)) {
            return null;
        }
        if (isMutator(method)) {
            return getMutatorParameterType(method);
        }
        if (isAccessor(method)) {
            return getAccessorReturnType(method);
        }
        return null;
    }

    private static Class<?> getMutatorParameterType(Method method) {
        checkArgument(isValidMutator(method), "Method '%s' on class '%s' is not a valid mutator", method.getName(), method.getDeclaringClass().getCanonicalName());
        return method.getParameterTypes()[0];
    }

    private static Class<?> getAccessorReturnType(Method method) {
        checkArgument(isValidAccessor(method), "Method '%s' on class '%s' is not a valid accessor", method.getName(), method.getDeclaringClass().getCanonicalName());
        return method.getReturnType();
    }

    public static String getCallingClassName(int depth) {
        StackTraceElement[] stack = new Exception().getStackTrace();
        return stack[depth + 2].getClassName();
    }

    public static List<String> getSearchableFields(EntityManager manager, Class<? extends RawEntity<?>> type) {
        List<String> back = new ArrayList<String>();

        for (Method m : type.getMethods()) {
            Searchable annot = getAnnotationDelegate(manager.getNameConverters().getFieldNameConverter(), m).getAnnotation(Searchable.class);

            if (annot != null) {
                Class<?> attributeType = Common.getAttributeTypeFromMethod(m);
                String name = manager.getNameConverters().getFieldNameConverter().getName(m);

                // don't index Entity fields
                if (name != null && !RawEntity.class.isAssignableFrom(attributeType) && !back.contains(name)) {
                    back.add(name);
                }
            }
        }

        return back;
    }

    private static Method getPrimaryKeyAccessor(Class<? extends RawEntity<?>> type) {
        final Iterable<Method> methods = MethodFinder.getInstance().findAnnotatedMethods(PrimaryKey.class, type);
        if (Iterables.isEmpty(methods)) {
            throw new RuntimeException("Entity " + type.getSimpleName() + " has no primary key field");
        }

        for (Method method : methods) {
            if (!method.getReturnType().equals(Void.TYPE) && method.getParameterTypes().length == 0) {
                return method;
            }
        }

        return null;
    }

    public static String getPrimaryKeyField(Class<? extends RawEntity<?>> type, FieldNameConverter converter) {
        final Iterable<Method> methods = MethodFinder.getInstance().findAnnotatedMethods(PrimaryKey.class, type);
        if (Iterables.isEmpty(methods)) {
            throw new RuntimeException("Entity " + type.getSimpleName() + " has no primary key field");
        }
        return converter.getName(methods.iterator().next());
    }

    public static <K> TypeInfo<K> getPrimaryKeyType(TypeManager typeManager, Class<? extends RawEntity<K>> type) {
        return typeManager.getType(getPrimaryKeyClassType(type));
    }

    public static <K> Class<K> getPrimaryKeyClassType(Class<? extends RawEntity<K>> type) {
        final Iterable<Method> methods = MethodFinder.getInstance().findAnnotatedMethods(PrimaryKey.class, type);
        if (Iterables.isEmpty(methods)) {
            throw new RuntimeException("Entity " + type.getSimpleName() + " has no primary key field");
        }

        final Method m = methods.iterator().next();

        Class<K> keyType = (Class<K>) m.getReturnType();
        if (keyType.equals(Void.TYPE)) {
            keyType = (Class<K>) m.getParameterTypes()[0];
        }
        return keyType;
    }

    public static <K> K getPrimaryKeyValue(RawEntity<K> entity) {
        if (entity instanceof EntityProxyAccessor) {
            return (K) ((EntityProxyAccessor) entity).getEntityProxy().getKey();
        }
        try {
            return (K) Common.getPrimaryKeyAccessor(entity.getEntityType()).invoke(entity);
        } catch (IllegalArgumentException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        } catch (InvocationTargetException e) {
            return null;
        }
    }

    public static <K> void validatePrimaryKey(FieldInfo<K> primaryKeyInfo, Object value) {
        if (null == value) {
            throw new IllegalArgumentException("Cannot set primary key to NULL");
        }

        TypeInfo<K> typeInfo = primaryKeyInfo.getTypeInfo();
        Class<K> javaTypeClass = primaryKeyInfo.getJavaType();

        if (!typeInfo.isAllowedAsPrimaryKey()) {
            throw new ActiveObjectsException(javaTypeClass.getName() + " cannot be used as a primary key!");
        }

        typeInfo.getLogicalType().validate(value);

        if ((value instanceof String) && StringUtils.isBlank((String) value)) {
            throw new ActiveObjectsException("Cannot set primary key to blank String");
        }
    }

    public static boolean fuzzyCompare(TypeManager typeManager, Object a, Object b) {
        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {    // implicitly, one or other is null, not both
            return false;
        }

        Object array = null;
        Object other = null;

        if (a.getClass().isArray()) {
            array = a;
            other = b;
        } else if (b.getClass().isArray()) {
            array = b;
            other = a;
        }

        if (array != null) {
            for (int i = 0; i < Array.getLength(array); i++) {
                if (fuzzyCompare(typeManager, Array.get(array, i), other)) {
                    return true;
                }
            }
        }

        return typeManager.getType(a.getClass()).getLogicalType().valueEquals(a, b)
                || typeManager.getType(b.getClass()).getLogicalType().valueEquals(b, a);
    }

    public static boolean fuzzyTypeCompare(int typeA, int typeB) {
        if (typeA == Types.BOOLEAN) {
            switch (typeB) {
                case Types.BIGINT:
                    return true;

                case Types.BIT:
                    return true;

                case Types.INTEGER:
                    return true;

                case Types.NUMERIC:
                    return true;

                case Types.SMALLINT:
                    return true;

                case Types.TINYINT:
                    return true;
            }
        }

        if ((typeA == Types.BIGINT || typeA == Types.BIT || typeA == Types.INTEGER || typeA == Types.NUMERIC
                || typeA == Types.SMALLINT || typeA == Types.TINYINT) && typeB == Types.BOOLEAN) {
            return true;
        } else if (typeA == Types.CLOB && (typeB == Types.LONGVARCHAR || typeB == Types.VARCHAR)) {
            return true;
        } else if ((typeA == Types.LONGVARCHAR || typeA == Types.VARCHAR) && typeB == Types.CLOB) {
            return true;
        } else if ((typeA == Types.BIGINT || typeA == Types.BIT || typeA == Types.DECIMAL || typeA == Types.DOUBLE
                || typeA == Types.FLOAT || typeA == Types.INTEGER || typeA == Types.REAL || typeA == Types.SMALLINT
                || typeA == Types.TINYINT) && typeB == Types.NUMERIC) {
            return true;
        } else if (typeA == Types.VARCHAR && typeB == Types.NVARCHAR) {
            return true;
        }

        return typeA == typeB;
    }

    /**
     * Gets all the methods of an entity that correspond to a value field. This means fields that are stored as values
     * in the database as opposed to fields (IDs) that define a relationship to another table in the database.
     * Note that the values are retrieved based on the relationship annotations, at the field level,
     * which the user may not have entered.
     *
     * @param entity    the entity to look up the methods from
     * @param converter the field name converter currently in use for entities
     * @return the set of method found
     */
    public static Set<Method> getValueFieldsMethods(final Class<? extends RawEntity<?>> entity, final FieldNameConverter converter) {
        return Sets.filter(Sets.newHashSet(entity.getMethods()), new Predicate<Method>() {
            public boolean apply(Method m) {
                final AnnotationDelegate annotations = getAnnotationDelegate(converter, m);
                return !annotations.isAnnotationPresent(Ignore.class)
                        && !annotations.isAnnotationPresent(OneToOne.class)
                        && !annotations.isAnnotationPresent(OneToMany.class)
                        && !annotations.isAnnotationPresent(ManyToMany.class);
            }
        });
    }

    /**
     * Gets all the names of fields of an entity that correspond to a value field. This means fields that are stored as
     * values in the database as opposed to fields (IDs) that define a relationship to another table in the database.
     *
     * @param entityInfo the entity to look up the methods from
     * @return the set of names found
     */
    public static ImmutableSet<String> getValueFieldsNames(final EntityInfo<? extends RawEntity<?>, ?> entityInfo, final FieldNameConverter converter) {
        List<String> valueFieldsNames = new ArrayList<String>();

        for (FieldInfo fieldInfo : entityInfo.getFields()) {
            // filter out just the value fields - we need to remove any entities from polymorphic relationships
            if (!Entity.class.isAssignableFrom(fieldInfo.getJavaType())) {
                // apply the name converter if we have a getter
                if (fieldInfo.hasAccessor()) {
                    valueFieldsNames.add(converter.getName(fieldInfo.getAccessor()));
                }
            }
        }

        return ImmutableSet.copyOf(valueFieldsNames);
    }

    public static List<String> preloadValue(Preload preload, final FieldNameConverter fnc) {
        final List<String> value = newArrayList(preload.value());
        if (fnc instanceof FieldNameProcessor) {
            return Lists.transform(value, new Function<String, String>() {
                @Override
                public String apply(String from) {
                    return ((FieldNameProcessor) fnc).convertName(from);
                }
            });
        } else {
            return value;
        }
    }

    public static String where(OneToOne oneToOne, final FieldNameConverter fnc) {
        return where(oneToOne.where(), fnc);
    }

    public static String where(OneToMany oneToMany, final FieldNameConverter fnc) {
        return where(oneToMany.where(), fnc);
    }

    public static String where(ManyToMany manyToMany, final FieldNameConverter fnc) {
        return where(manyToMany.where(), fnc);
    }

    private static String where(String where, final FieldNameConverter fnc) {
        if (fnc instanceof FieldNameProcessor) {
            final Matcher matcher = SqlUtils.WHERE_CLAUSE.matcher(where);
            final StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(sb, convert(fnc, matcher.group(1)));
            }
            matcher.appendTail(sb);
            return sb.toString();
        } else {
            return where;
        }
    }

    public static String convert(FieldNameConverter fnc, String column) {
        if (fnc instanceof FieldNameProcessor) {
            return ((FieldNameProcessor) fnc).convertName(column);
        } else {
            return column;
        }
    }

    /**
     * Closes the {@link java.sql.ResultSet} in a {@code null} safe manner and quietly, i.e without throwing nor logging
     * any exception
     *
     * @param resultSet the result set to close
     */
    @Deprecated
    public static void closeQuietly(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // ignored
            }
        }
    }

    /**
     * Closes the {@link java.sql.Statement} in a {@code null} safe manner and quietly, i.e without throwing nor logging
     * any exception
     *
     * @param statement the statement to close
     */
    @Deprecated
    public static void closeQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignored
            }
        }
    }

    /**
     * Closes the {@link java.sql.Connection} in a {@code null} safe manner and quietly, i.e without throwing nor logging
     * any exception
     *
     * @param connection the connection to close, can be {@code null}
     */
    @Deprecated
    public static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignored
            }
        }
    }

    /**
     * Shortens a String to the given length if necessary. The process of shortening is stable.
     */
    public static String shorten(String s, int length) {
        if (s == null || s.length() <= length) {
            return s;
        }

        final int tailLength = length / 3;
        final int hash = Math.abs((int) (s.hashCode() % Math.round(Math.pow(10, tailLength))));

        return s.substring(0, length - tailLength - 1) + hash;
    }
}
