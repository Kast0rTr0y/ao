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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.AnnotationDelegate;
import net.java.ao.Common;
import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Sets.*;
import static net.java.ao.sql.SqlUtils.closeQuietly;

/**
 * WARNING: <i>Not</i> part of the public API.  This class is public only
 * to allow its use within other packages in the ActiveObjects library.
 *
 * @author Daniel Spiewak
 */
public final class SchemaGenerator
{
    private static final Set<Integer> AUTO_INCREMENT_LEGAL_TYPES = ImmutableSet.of(Types.INTEGER, Types.BIGINT);

    public static void migrate(DatabaseProvider provider,
                               SchemaConfiguration schemaConfiguration,
                               NameConverters nameConverters,
                               Class<? extends RawEntity<?>>... classes) throws SQLException
    {
        final Iterable<String> statements = generateImpl(provider, schemaConfiguration, nameConverters, classes);

        Connection conn = null;
        Statement stmt = null;
        try
        {
            conn = provider.getConnection();
            stmt = conn.createStatement();
            for (String statement : statements)
            {
                provider.executeUpdate(stmt, statement);
            }
        }
        finally
        {
            closeQuietly(stmt, conn);
        }
    }

    private static Iterable<String> generateImpl(DatabaseProvider provider,
                                                 SchemaConfiguration schemaConfiguration,
                                                 NameConverters nameConverters,
                                                 Class<? extends RawEntity<?>>... classes) throws SQLException
    {
        final Collection<String> statements = newLinkedHashSet(); // preserve the order of the elements

        final DDLTable[] parsedTables = parseDDL(nameConverters, classes);
        final DDLTable[] readTables = SchemaReader.readSchema(provider, nameConverters, schemaConfiguration);

        final DDLAction[] actions = SchemaReader.sortTopologically(SchemaReader.diffSchema(parsedTables, readTables, provider.isCaseSensetive()));
        for (DDLAction action : actions)
        {
            statements.addAll(Arrays.asList(provider.renderAction(nameConverters, action)));
        }
        return filterEmpty(statements);
    }

    private static Iterable<String> filterEmpty(Iterable<String> iterable)
    {
        return Iterables.filter(iterable, new Predicate<String>()
        {
            @Override
            public boolean apply(String input)
            {
                return !input.trim().isEmpty();
            }
        });
    }

    static DDLTable[] parseDDL(NameConverters nameConverters, Class<? extends RawEntity<?>>... classes) {
		final Map<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>> deps = new HashMap<Class<? extends RawEntity<?>>, Set<Class<? extends RawEntity<?>>>>();
		final Set<Class<? extends RawEntity<?>>> roots = new LinkedHashSet<Class<? extends RawEntity<?>>>();

		for (Class<? extends RawEntity<?>> cls : classes) {
			try {
				parseDependencies(nameConverters.getFieldNameConverter(), deps, roots, cls);
			} catch (StackOverflowError e) {
				throw new RuntimeException("Circular dependency detected in or below " + cls.getCanonicalName());
			}
		}

		List<DDLTable> parsedTables = new ArrayList<DDLTable>();

		while (!roots.isEmpty()) {
			Class<? extends RawEntity<?>>[] rootsArray = roots.toArray(new Class[roots.size()]);
			roots.remove(rootsArray[0]);

			Class<? extends RawEntity<?>> clazz = rootsArray[0];
			if (clazz.getAnnotation(Polymorphic.class) == null) {
				parsedTables.add(parseInterface(nameConverters.getTableNameConverter(), nameConverters.getFieldNameConverter(), clazz));
			}

			List<Class<? extends RawEntity<?>>> toRemove = new LinkedList<Class<? extends RawEntity<?>>>();
			Iterator<Class<? extends RawEntity<?>>> depIterator = deps.keySet().iterator();
			while (depIterator.hasNext()) {
				Class<? extends RawEntity<?>> depClass = depIterator.next();

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

		return parsedTables.toArray(new DDLTable[parsedTables.size()]);
	}

	private static void parseDependencies(FieldNameConverter fieldConverter, Map<Class <? extends RawEntity<?>>,
			Set<Class<? extends RawEntity<?>>>> deps, Set<Class <? extends RawEntity<?>>> roots, Class<? extends RawEntity<?>>... classes) {
		for (Class<? extends RawEntity<?>> clazz : classes) {
			if (deps.containsKey(clazz)) {
				continue;
			}

			Set<Class<? extends RawEntity<?>>> individualDeps = new LinkedHashSet<Class<? extends RawEntity<?>>>();

			for (Method method : clazz.getMethods()) {
				String attributeName = fieldConverter.getName(method);
				Class<?> type = Common.getAttributeTypeFromMethod(method);

				if (attributeName != null && type != null && Common.interfaceInheritsFrom(type, RawEntity.class)) {
					if (!type.equals(clazz)) {
						individualDeps.add((Class<? extends RawEntity<?>>) type);

						parseDependencies(fieldConverter, deps, roots, (Class<? extends RawEntity<?>>) type);
					}
				}
			}

			if (individualDeps.size() == 0) {
				roots.add(clazz);
			} else {
				deps.put(clazz, individualDeps);
			}
		}
	}

	private static DDLTable parseInterface(TableNameConverter nameConverter, FieldNameConverter fieldConverter, Class<? extends RawEntity<?>> clazz)
    {
		String sqlName = nameConverter.getName(clazz);

		DDLTable table = new DDLTable();
		table.setName(sqlName);

		table.setFields(parseFields(clazz, fieldConverter));
		table.setForeignKeys(parseForeignKeys(nameConverter, fieldConverter, clazz));
		table.setIndexes(parseIndexes(nameConverter, fieldConverter, clazz));

		return table;
	}

	/**
	 * Not intended for public usage.  This method is declared <code>public</code>
	 * only to enable use within other ActiveObjects packages.  Consider this
	 * function <b>unsupported</b>.
	 */
	public static DDLField[] parseFields(Class<? extends RawEntity<?>> clazz, FieldNameConverter fieldConverter) {
		List<DDLField> fields = new ArrayList<DDLField>();
		List<String> attributes = new LinkedList<String>();

		for (Method method : Common.getValueFieldsMethods(clazz, fieldConverter))
        {
			String attributeName = fieldConverter.getName(method);
			final Class<?> type = Common.getAttributeTypeFromMethod(method);

			if (attributeName != null && type != null)
            {
				if (attributes.contains(attributeName)) {
					continue;
				}
				attributes.add(attributeName);

                final AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldConverter, method);

                DDLField field = new DDLField();
                field.setName(attributeName);

                final DatabaseType<?> sqlType = getSQLTypeFromMethod(type, annotations);
                field.setType(sqlType);

                field.setPrecision(getPrecisionFromMethod(type, method, fieldConverter));
                field.setScale(getScaleFromMethod(type, method, fieldConverter));
                field.setPrimaryKey(annotations.isAnnotationPresent(PrimaryKey.class));
                field.setNotNull(annotations.isAnnotationPresent(NotNull.class) || annotations.isAnnotationPresent(Unique.class) || annotations.isAnnotationPresent(PrimaryKey.class));
                field.setUnique(annotations.isAnnotationPresent(Unique.class));

                final boolean isAutoIncrement = isAutoIncrement(type, annotations, field.getType());
                field.setAutoIncrement(isAutoIncrement);

                if (!isAutoIncrement && annotations.isAnnotationPresent(Default.class))
                {
                    field.setDefaultValue(convertStringValue(annotations.getAnnotation(Default.class).value(), sqlType));
                }

                if (annotations.isAnnotationPresent(OnUpdate.class))
                {
                    field.setOnUpdate(convertStringValue(annotations.getAnnotation(OnUpdate.class).value(), sqlType));
                }

                if (field.isPrimaryKey()) {
					fields.add(0, field);
				} else {
					fields.add(field);
				}

				if (Common.interfaceInheritsFrom(type, RawEntity.class)
						&& type.getAnnotation(Polymorphic.class) != null) {
					field.setDefaultValue(null);		// polymorphic fields can't have default
					field.setOnUpdate(null);		// or on update

					attributeName = fieldConverter.getPolyTypeName(method);

					field = new DDLField();

					field.setName(attributeName);
					field.setType(TypeManager.getInstance().getType(String.class));
					field.setPrecision(127);
					field.setScale(-1);

					if (annotations.getAnnotation(NotNull.class) != null) {
						field.setNotNull(true);
					}

					fields.add(field);
				}
			}
		}

		return fields.toArray(new DDLField[fields.size()]);
	}

    private static boolean isAutoIncrement(Class<?> type, AnnotationDelegate annotations, DatabaseType<?> dbType)
    {
        final int sqlType1 = dbType.getType();
        final boolean isAutoIncrement = annotations.isAnnotationPresent(AutoIncrement.class);
        if (isAutoIncrement && (!AUTO_INCREMENT_LEGAL_TYPES.contains(sqlType1) || type.isEnum()))
        {
            throw new ActiveObjectsConfigurationException(AutoIncrement.class.getName() + " is not supported for type: " + dbType + " corresponding to SQL type: " + sqlType1);
        }
        return isAutoIncrement;
    }

    private static DatabaseType<?> getSQLTypeFromMethod(Class<?> type, AnnotationDelegate annotations) {
		DatabaseType<?> sqlType = null;
		TypeManager manager = TypeManager.getInstance();

		sqlType = manager.getType(type);

		SQLType sqlTypeAnnotation = annotations.getAnnotation(SQLType.class);
		if (sqlTypeAnnotation != null) {
			final int annoType = sqlTypeAnnotation.value();

			if (annoType != Types.NULL) {
				sqlType = manager.getType(annoType);
			}
		}

		return sqlType;
	}

	private static int getPrecisionFromMethod(Class<?> type, Method method, FieldNameConverter converter) {
		TypeManager manager = TypeManager.getInstance();
		int precision = -1;

		precision = manager.getType(type).getDefaultPrecision();

		SQLType sqlTypeAnnotation = Common.getAnnotationDelegate(converter, method).getAnnotation(SQLType.class);
		if (sqlTypeAnnotation != null) {
			precision = sqlTypeAnnotation.precision();
		}

		return precision;
	}

	private static int getScaleFromMethod(Class<?> type, Method method, FieldNameConverter converter) {
		int scale = -1;

		SQLType sqlTypeAnnotation = Common.getAnnotationDelegate(converter, method).getAnnotation(SQLType.class);
		if (sqlTypeAnnotation != null) {
			scale = sqlTypeAnnotation.scale();
		}

		return scale;
	}

	private static DDLForeignKey[] parseForeignKeys(TableNameConverter nameConverter, FieldNameConverter fieldConverter,
			Class<? extends RawEntity<?>> clazz) {
		Set<DDLForeignKey> back = new LinkedHashSet<DDLForeignKey>();

		for (Method method : clazz.getMethods()) {
			String attributeName = fieldConverter.getName(method);
			Class<?> type =  Common.getAttributeTypeFromMethod(method);

			if (type != null && attributeName != null && Common.interfaceInheritsFrom(type, RawEntity.class)
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

	private static DDLIndex[] parseIndexes(TableNameConverter nameConverter, FieldNameConverter fieldConverter,
			Class<? extends RawEntity<?>> clazz) {
		Set<DDLIndex> back = new LinkedHashSet<DDLIndex>();
		String tableName = nameConverter.getName(clazz);

		for (Method method : clazz.getMethods()) {
			String attributeName = fieldConverter.getName(method);
			AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldConverter, method);

			if (Common.isAccessor(method) || Common.isMutator(method)) {
				Indexed indexedAnno = annotations.getAnnotation(Indexed.class);
				Class<?> type = Common.getAttributeTypeFromMethod(method);

				if (indexedAnno != null || (type != null && Common.interfaceInheritsFrom(type, RawEntity.class))) {
					DDLIndex index = new DDLIndex();
					index.setField(attributeName);
					index.setTable(tableName);
					index.setType(getSQLTypeFromMethod(type, annotations));

					back.add(index);
				}
			}
		}

		for (Class<?> superInterface : clazz.getInterfaces()) {
			if (!superInterface.equals(RawEntity.class) && !superInterface.isAnnotationPresent(Polymorphic.class)) {
				back.addAll(Arrays.asList(parseIndexes(nameConverter, fieldConverter, (Class<? extends RawEntity<?>>) superInterface)));
			}
		}

		return back.toArray(new DDLIndex[back.size()]);
	}

	private static Object convertStringValue(String value, DatabaseType<?> type) {
		if (value == null) {
			return null;
		} else if (value.trim().equalsIgnoreCase("NULL")) {
			return value.trim();
		}

		DatabaseFunction func = DatabaseFunction.get(value.trim());
		if (func != null) {
			return func;
		}

		return type.defaultParseValue(value);
	}
}
