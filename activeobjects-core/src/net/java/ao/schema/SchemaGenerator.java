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
package net.java.ao.schema;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.AnnotationDelegate;
import net.java.ao.Common;
import net.java.ao.DatabaseProvider;
import net.java.ao.ManyToMany;
import net.java.ao.OneToMany;
import net.java.ao.OneToOne;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;
import net.java.ao.types.TypeQualifiers;
import net.java.ao.util.EnumUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayList;
import static net.java.ao.types.TypeQualifiers.MAX_STRING_LENGTH;
import static net.java.ao.types.TypeQualifiers.qualifiers;

/**
 * WARNING: <i>Not</i> part of the public API.  This class is public only
 * to allow its use within other packages in the ActiveObjects library.
 *
 * @author Daniel Spiewak
 */
public final class SchemaGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaGenerator.class);

    private static final Set<Integer> AUTO_INCREMENT_LEGAL_TYPES = ImmutableSet.of(Types.INTEGER, Types.BIGINT);

    public static void migrate(DatabaseProvider provider,
                               SchemaConfiguration schemaConfiguration,
                               NameConverters nameConverters,
                               final boolean executeDestructiveUpdates,
                               Class<? extends RawEntity<?>>... classes) throws SQLException
    {
        final Iterable<Iterable<SQLAction>> actionGroups = generateImpl(provider, schemaConfiguration, nameConverters, executeDestructiveUpdates, classes);
        final Connection conn = provider.getConnection();
        try
        {
            final Statement stmt = conn.createStatement();
            try
            {
                Set<String> completedStatements = new HashSet<String>();
                for (Iterable<SQLAction> actionGroup : actionGroups)
                {
                    addAll(completedStatements, provider.executeUpdatesForActions(stmt, actionGroup, completedStatements));
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

    private static Iterable<Iterable<SQLAction>> generateImpl(final DatabaseProvider provider,
                                                 final SchemaConfiguration schemaConfiguration,
                                                 final NameConverters nameConverters,
                                                 final boolean executeDestructiveUpdates,
                                                 Class<? extends RawEntity<?>>... classes) throws SQLException
    {
        final DDLTable[] parsedTables = parseDDL(provider, nameConverters, classes);
        final DDLTable[] readTables = SchemaReader.readSchema(provider, nameConverters, schemaConfiguration);

        final DDLAction[] actions = SchemaReader.sortTopologically(SchemaReader.diffSchema(provider.getTypeManager(), parsedTables, readTables, provider.isCaseSensitive()));
        return Iterables.transform(Iterables.filter(ImmutableList.copyOf(actions), new Predicate<DDLAction>() {

            @Override
            public boolean apply(final DDLAction input) {
                switch (input.getActionType()) {
                    case DROP:
                    case ALTER_DROP_COLUMN:
                        return executeDestructiveUpdates;
                    default:
                        return true;
                }
            }

        }),
                new Function<DDLAction, Iterable<SQLAction>>()
                {
                    public Iterable<SQLAction> apply(DDLAction from)
                    {
                        return provider.renderAction(nameConverters, from);
                    }
                });
    }

    static DDLTable[] parseDDL(DatabaseProvider provider, NameConverters nameConverters, Class<? extends RawEntity<?>>... classes) {
		final Map<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>> deps = new HashMap<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>>();
		final Set<Class<? extends RawEntity<?>>> roots = new LinkedHashSet<Class<? extends RawEntity<?>>>();

        for (Class<? extends RawEntity<?>> cls : classes) {
            parseDependencies(nameConverters.getFieldNameConverter(), deps, roots, cls);
        }

        ArrayList<DDLTable> parsedTables = new ArrayList<DDLTable>();

        parseDDLRoots(provider, nameConverters, deps, roots, parsedTables);
        if (!deps.isEmpty()) {
            throw new RuntimeException("Circular dependency detected");
        }

		return parsedTables.toArray(new DDLTable[parsedTables.size()]);
	}

    private static void parseDDLRoots(final DatabaseProvider provider,
                                      final NameConverters nameConverters,
                                      final Map<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>> deps,
                                      final Set<Class<? extends RawEntity<?>>> roots,
                                      final ArrayList<DDLTable> parsedTables) {
        while (!roots.isEmpty()) {
            Class<? extends RawEntity<?>> clazz = roots.iterator().next();
            roots.remove(clazz);

            if (clazz.getAnnotation(Polymorphic.class) == null) {
                parsedTables.add(parseInterface(provider, nameConverters.getTableNameConverter(), nameConverters.getFieldNameConverter(), clazz));
            }

            List<Class<? extends RawEntity<?>>> toRemove = new LinkedList<Class<? extends RawEntity<?>>>();
            for (final Class<? extends RawEntity<?>> depClass : deps.keySet()) {
                Set<Class<? extends RawEntity<?>>> individualDeps = deps.get(depClass);
                individualDeps.remove(clazz);

                if (individualDeps.isEmpty()) {
                    roots.add(depClass);
                    toRemove.add(depClass);
                }
            }

            for (Class<? extends RawEntity<?>> remove : toRemove) {
                deps.remove(remove);
            }
        }
    }

    private static void parseDependencies(
            final FieldNameConverter fieldConverter,
            final Map<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>> deps,
            final Set<Class<? extends RawEntity<?>>> roots, Class<? extends RawEntity<?>> clazz)
    {
        if (deps.containsKey(clazz))
        {
            return;
        }
        final Set<Class<? extends RawEntity<?>>> individualDeps = new LinkedHashSet<Class<? extends RawEntity<?>>>();
        for (final Method method : clazz.getMethods())
        {
            final Class<?> type = Common.getAttributeTypeFromMethod(method);
            validateManyToManyAnnotation(method);
            validateOneToOneAnnotation(method);
            validateOneToManyAnnotation(method);
            if (fieldConverter.getName(method) != null && type != null && !type.equals(clazz) &&
                RawEntity.class.isAssignableFrom(type) && !individualDeps.contains(type))
            {
                individualDeps.add((Class<? extends RawEntity<?>>) type);
                addDeps(deps, clazz, individualDeps);
                parseDependencies(fieldConverter, deps, roots, (Class<? extends RawEntity<?>>) type);
            }
        }

        if (individualDeps.size() == 0)
        {
            roots.add(clazz);
        }
        else
        {
            addDeps(deps, clazz, individualDeps);
        }
    }

    private static void addDeps(final Map<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>> deps,
                                final Class<? extends RawEntity<?>> clazz,
                                final Set<Class<? extends RawEntity<?>>> individualDeps)
    {
        Set<Class<? extends RawEntity<?>>> classes = deps.get(clazz);
        if (classes != null) {
            classes.addAll(individualDeps);
        } else {
            deps.put(clazz, individualDeps);
        }
    }

    private static void validateManyToManyAnnotation(final Method method)
    {
        final ManyToMany manyToMany = method.getAnnotation(ManyToMany.class);
        if (manyToMany != null)
        {
            final Class<? extends RawEntity<?>> throughType = manyToMany.value();
            final String reverse = manyToMany.reverse();
            if (reverse.length() != 0)
            {
                try
                {
                    throughType.getMethod(reverse);
                }
                catch (final NoSuchMethodException exception)
                {
                    throw new IllegalArgumentException(method + " has a ManyToMany annotation with an invalid reverse element value. It must be the name of the corresponding getter method on the joining entity.", exception);
                }
            }
            if (manyToMany.through().length() != 0)
            {
                try
                {
                    throughType.getMethod(manyToMany.through());
                }
                catch (final NoSuchMethodException exception)
                {
                    throw new IllegalArgumentException(method + " has a ManyToMany annotation with an invalid through element value. It must be the name of the getter method on the joining entity that refers to the remote entities.", exception);
                }
            }
        }
    }

    private static void validateOneToManyAnnotation(final Method method)
    {
        final OneToMany oneToMany = method.getAnnotation(OneToMany.class);
        if (oneToMany != null)
        {
            final String reverse = oneToMany.reverse();
            if (reverse.length() != 0)
            {
                try
                {
                    method.getReturnType().getComponentType().getMethod(reverse);
                }
                catch (final NoSuchMethodException exception)
                {
                    throw new IllegalArgumentException(method + " has a OneToMany annotation with an invalid reverse element value. It must be the name of the corresponding getter method on the related entity.", exception);
                }
            }
        }
    }

    private static void validateOneToOneAnnotation(final Method method)
    {
        final OneToOne oneToOne = method.getAnnotation(OneToOne.class);
        if (oneToOne != null)
        {
            final String reverse = oneToOne.reverse();
            if (reverse.length() != 0)
            {
                try
                {
                    method.getReturnType().getMethod(reverse);
                }
                catch (final NoSuchMethodException exception)
                {
                    throw new IllegalArgumentException(method + " has OneToMany annotation with an invalid reverse element value. It be the name of the corresponding getter method on the related entity.", exception);
                }
            }
        }
    }

    /**
     * Not intended for public use.
     */
    public static DDLTable parseInterface(DatabaseProvider provider, TableNameConverter nameConverter, FieldNameConverter fieldConverter, Class<? extends RawEntity<?>> clazz)
    {
		String sqlName = nameConverter.getName(clazz);

		DDLTable table = new DDLTable();
		table.setName(sqlName);

		table.setFields(parseFields(provider, fieldConverter, clazz));
		table.setForeignKeys(parseForeignKeys(nameConverter, fieldConverter, clazz));
		table.setIndexes(parseIndexes(provider, nameConverter, fieldConverter, clazz));

		return table;
	}

	/**
	 * Not intended for public usage.  This method is declared <code>public</code>
	 * only to enable use within other ActiveObjects packages.  Consider this
	 * function <b>unsupported</b>.
	 */
	public static DDLField[] parseFields(DatabaseProvider provider, FieldNameConverter fieldConverter, Class<? extends RawEntity<?>> clazz) {
		List<DDLField> fields = new ArrayList<DDLField>();
		List<String> attributes = new LinkedList<String>();

		for (Method method : Common.getValueFieldsMethods(clazz, fieldConverter))
        {
			String attributeName = fieldConverter.getName(method);
			final Class<?> type = Common.getAttributeTypeFromMethod(method);

			if (attributeName != null && type != null)
            {
                checkIsSupportedType(method, type);

				if (attributes.contains(attributeName)) {
					continue;
				}
				attributes.add(attributeName);

                final AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldConverter, method);

                DDLField field = new DDLField();
                field.setName(attributeName);

                final TypeManager typeManager = provider.getTypeManager();
                final TypeInfo<?> sqlType = getSQLTypeFromMethod(typeManager, type, method, annotations);
                field.setType(sqlType);
                field.setJdbcType(sqlType.getJdbcWriteType());

                field.setPrimaryKey(isPrimaryKey(annotations, field));

                field.setNotNull(annotations.isAnnotationPresent(NotNull.class) || annotations.isAnnotationPresent(Unique.class) || annotations.isAnnotationPresent(PrimaryKey.class));
                field.setUnique(annotations.isAnnotationPresent(Unique.class));

                final boolean isAutoIncrement = isAutoIncrement(type, annotations, field.getType());
                field.setAutoIncrement(isAutoIncrement);

                if (!isAutoIncrement)
                {
                    if (annotations.isAnnotationPresent(Default.class))
                    {
                        final Object defaultValue = convertStringDefaultValue(annotations.getAnnotation(Default.class).value(), sqlType, method);
                        if (type.isEnum() && ((Integer) defaultValue) > EnumUtils.size((Class<? extends Enum>) type) - 1)
                        {
                            throw new ActiveObjectsConfigurationException("There is no enum value of '" + type + "'for which the ordinal is " + defaultValue);
                        }
                        field.setDefaultValue(defaultValue);
                    }
                    else if (ImmutableSet.<Class<?>>of(short.class, float.class, int.class, long.class, double.class).contains(type))
                    {
                        // set the default value for primitive types (float, short, int, long, char)
                        field.setDefaultValue(convertStringDefaultValue("0", sqlType, method));
                    }
                }

                if (field.isPrimaryKey()) {
					fields.add(0, field);
				} else {
					fields.add(field);
				}

                if (RawEntity.class.isAssignableFrom(type)
						&& type.getAnnotation(Polymorphic.class) != null) {
					field.setDefaultValue(null);		// polymorphic fields can't have default

					attributeName = fieldConverter.getPolyTypeName(method);

					field = new DDLField();

					field.setName(attributeName);
					field.setType(typeManager.getType(String.class, qualifiers().stringLength(127)));
                    field.setJdbcType(java.sql.Types.VARCHAR);

					if (annotations.getAnnotation(NotNull.class) != null) {
						field.setNotNull(true);
					}

					fields.add(field);
				}
			}
		}

		return fields.toArray(new DDLField[fields.size()]);
	}

    private static void checkIsSupportedType(Method method, Class<?> type)
    {
        if (type.equals(java.sql.Date.class))
        {
            throw new ActiveObjectsConfigurationException(Date.class.getName()
                    + " is not supported! Please use " + java.util.Date.class.getName() + " instead.")
                    .forMethod(method);
        }
    }

    private static boolean isPrimaryKey(AnnotationDelegate annotations, DDLField field)
    {
        final boolean isPrimaryKey = annotations.isAnnotationPresent(PrimaryKey.class);
        if (isPrimaryKey && !field.getType().isAllowedAsPrimaryKey())
        {
            throw new ActiveObjectsConfigurationException(PrimaryKey.class.getName() + " is not supported for type: " + field.getType());
        }
        return isPrimaryKey;
    }

    private static boolean isAutoIncrement(Class<?> type, AnnotationDelegate annotations, TypeInfo<?> dbType)
    {
        final boolean isAutoIncrement = annotations.isAnnotationPresent(AutoIncrement.class);
        if (isAutoIncrement && (!AUTO_INCREMENT_LEGAL_TYPES.contains(dbType.getJdbcWriteType()) || type.isEnum()))
        {
            throw new ActiveObjectsConfigurationException(AutoIncrement.class.getName() + " is not supported for type: " + dbType);
        }
        return isAutoIncrement;
    }

    private static TypeInfo<?> getSQLTypeFromMethod(TypeManager typeManager, Class<?> type, Method method, AnnotationDelegate annotations) {
		TypeQualifiers qualifiers = qualifiers();

		StringLength lengthAnno = annotations.getAnnotation(StringLength.class);
		if (lengthAnno != null) {
		    final int length = lengthAnno.value();
		    if (length > MAX_STRING_LENGTH)
		    {
		        throw new ActiveObjectsConfigurationException("@StringLength must be <= " + MAX_STRING_LENGTH + " or UNLIMITED").forMethod(method);
		    }
		    try {
		        qualifiers = qualifiers.stringLength(length);
		    }
		    catch (ActiveObjectsConfigurationException e) {
		        throw new ActiveObjectsConfigurationException(e.getMessage()).forMethod(method);
		    }
		}

		return typeManager.getType(type, qualifiers);
	}

    private static DDLForeignKey[] parseForeignKeys(TableNameConverter nameConverter, FieldNameConverter fieldConverter,
			Class<? extends RawEntity<?>> clazz) {
		Set<DDLForeignKey> back = new LinkedHashSet<DDLForeignKey>();

		for (Method method : clazz.getMethods()) {
			String attributeName = fieldConverter.getName(method);
			Class<?> type =  Common.getAttributeTypeFromMethod(method);

            if (type != null && attributeName != null && RawEntity.class.isAssignableFrom(type)
					&& type.getAnnotation(Polymorphic.class) == null) {
				DDLForeignKey key = new DDLForeignKey();

				key.setField(attributeName);
				key.setTable(nameConverter.getName((Class<? extends RawEntity<?>>) type));
				key.setForeignField(Common.getPrimaryKeyField((Class<? extends RawEntity<?>>) type, fieldConverter));
				key.setDomesticTable(nameConverter.getName(clazz));

				back.add(key);
			}
		}

		return back.toArray(new DDLForeignKey[back.size()]);
	}

    @VisibleForTesting
	static DDLIndex[] parseIndexes(DatabaseProvider provider, TableNameConverter nameConverter, FieldNameConverter fieldConverter,
                                           Class<? extends RawEntity<?>> clazz) {
		Set<DDLIndex> back = new LinkedHashSet<DDLIndex>();
		String tableName = nameConverter.getName(clazz);

		for (Method method : clazz.getMethods()) {
			String attributeName = fieldConverter.getName(method);
			AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldConverter, method);

			if (Common.isAccessor(method) || Common.isMutator(method)) {
				Indexed indexedAnno = annotations.getAnnotation(Indexed.class);
				Class<?> type = Common.getAttributeTypeFromMethod(method);

                if (indexedAnno != null || (type != null && RawEntity.class.isAssignableFrom(type))) {
					DDLIndex index = new DDLIndex();
					index.setField(attributeName);
					index.setTable(tableName);
					index.setType(getSQLTypeFromMethod(provider.getTypeManager(), type, method, annotations));

					back.add(index);
				}
			}
		}

		for (Class<?> superInterface : clazz.getInterfaces()) {
			if (RawEntity.class.isAssignableFrom(superInterface) &&
                    !(RawEntity.class.equals(superInterface) || superInterface.isAnnotationPresent(Polymorphic.class))) {
				back.addAll(Arrays.asList(parseIndexes(
                        provider,
                        nameConverter,
                        fieldConverter,
                        (Class<? extends RawEntity<?>>) superInterface)
                ));
			}
		}

		return back.toArray(new DDLIndex[back.size()]);
	}

    private static Object convertStringDefaultValue(String value, TypeInfo<?> type, Method method)
    {
        if (value == null)
        {
            return null;
        }
        if (!type.getSchemaProperties().isDefaultValueAllowed())
        {
            throw new ActiveObjectsConfigurationException("Default value is not allowed for database type " +
                type.getSchemaProperties().getSqlTypeName());
        }
        try
        {
            Object ret = type.getLogicalType().parseDefault(value);
            if (ret == null)
            {
                throw new ActiveObjectsConfigurationException("Default value cannot be empty").forMethod(method);
            }
            return ret;
        }
        catch (IllegalArgumentException e)
        {
            throw new ActiveObjectsConfigurationException(e.getMessage());
        }
    }
}
