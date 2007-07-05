/*
 * Copyright 2007, Daniel Spiewak
 * All rights reserved
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.
 *   * Neither the name of the ActiveObjects project nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package net.java.ao.schema;

import static net.java.ao.Common.getAttributeNameFromMethod;
import static net.java.ao.Common.getAttributeTypeFromMethod;
import static net.java.ao.Common.interfaceInheritsFrom;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;
import net.java.ao.Entity;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * @author Daniel Spiewak
 */
public final class Generator {
	private static final String CL_INVOCATION = "java -jar activeobjects-*.jar <options> class1 class2 ...";
	
	public static void main(String... args) throws ParseException, IOException, ClassNotFoundException {
		Options options = new Options();
		
		Option classpathOption = new Option("C", "classpath", true, "(required) The path from which to load the " +
				"specified entity classes");
		classpathOption.setRequired(true);
		options.addOption(classpathOption);
		
		Option nameConverterOption = new Option("N", "converter", true, "A qualified Java class-name specifying which " +
				"name converter to use");
		options.addOption(nameConverterOption);
		
		Option uriOption = new Option("U", "uri", true, "(required) The JDBC URI used to access the database against " +
				"which the schema will be created");
		uriOption.setRequired(true);
		options.addOption(uriOption);
		
		CommandLineParser parser = new PosixParser();
		CommandLine cl = null;
		try {
			cl = parser.parse(options, args);
		} catch (ParseException e) {
			new HelpFormatter().printHelp(CL_INVOCATION, options);
			System.exit(-1);
		}
		
		if (cl.getOptionValue(classpathOption.getOpt()) == null || cl.getArgs().length == 0) {
			new HelpFormatter().printHelp(CL_INVOCATION, options);
			System.exit(-1);
		}
		
		System.out.println(generate(cl.getOptionValue(classpathOption.getOpt()), cl.getOptionValue(uriOption.getOpt()), 
				cl.getOptionValue(nameConverterOption.getOpt()), cl.getArgs()));
	}
	
	public static String generate(String classpath, String uri, String... classes) throws ClassNotFoundException, MalformedURLException, IOException {
		return generate(classpath, uri, null, classes);
	}
	
	public static String generate(String classpath, String uri, String nameConverterClassname, 
			String... classes) throws ClassNotFoundException, MalformedURLException, IOException {
		ClassLoader classloader = new URLClassLoader(new URL[] {new URL("file://" + new File(classpath).getCanonicalPath() + "/")});
		
		String sql = "";
		DatabaseProvider provider = DatabaseProvider.getInstance(uri, null, null, false);
		
		String[] statements = generateImpl(provider, loadConverter(classloader, nameConverterClassname), classloader, classes);
		for (String statement : statements) {
			sql += statement + ";\n";
		}
		
		return sql;
	}
	
	public static void migrate(DatabaseProvider provider, Class<? extends Entity>... classes) throws SQLException {
		migrate(provider, new CamelCaseNameConverter(), classes);
	}
	
	public static void migrate(DatabaseProvider provider, PluggableNameConverter nameConverter,
			Class<? extends Entity>... classes) throws SQLException {
		List<String> classNames = new ArrayList<String>();
		
		for (Class<? extends Entity> clazz : classes) {
			classNames.add(clazz.getCanonicalName());
		}
		
		String[] statements = null;
		try {
			statements = generateImpl(provider, nameConverter, Generator.class.getClassLoader(), classNames.toArray(new String[classNames.size()]));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		Connection conn = provider.getConnection();
		try {
			Statement stmt = conn.createStatement();
			
			for (String statement : statements) {
				if (!statement.trim().equals("")) {
					Logger.getLogger("net.java.ao").log(Level.INFO, statement);
					stmt.executeUpdate(statement);
				}
			}
			
			stmt.close();
		} finally {
			conn.close();
		}
	}
	
	public static boolean hasSchema(DatabaseProvider provider, PluggableNameConverter nameConverter,
			Class<? extends Entity>... classes) throws SQLException {
		if (classes.length == 0) {
			return true;
		}
		
		Connection conn = provider.getConnection();
		try {
			Statement stmt = conn.createStatement();
			
			try {
				// TODO	kind of a hacky way to figure this out, but it works
				stmt.executeQuery("SELECT * FROM " + nameConverter.getName(classes[0])).close();
			} catch (SQLException e) {
				return false;
			}
			
			stmt.close();
		} finally {
			conn.close();
		}
		
		return true;
	}
	
	private static PluggableNameConverter loadConverter(ClassLoader classloader, String name) {
		if (name.split(".").length == 0) {
			name = "net.java.ao.schema." + name;
		}
		
		Class<? extends PluggableNameConverter> converterClass = null;
		
		try {
			converterClass = (Class<? extends PluggableNameConverter>) Class.forName(name);
		} catch (Throwable t) {
		}
		
		if (converterClass == null) {
			try {
				converterClass = (Class<? extends PluggableNameConverter>) Class.forName(name, true, classloader);
			} catch (Throwable t) {
			}
		}
		
		PluggableNameConverter nameConverter = null;
		try {
			nameConverter = converterClass.newInstance();
		} catch (Throwable t) {
			System.err.println("Unable to load " + name);
			System.err.println("Using default name converter...");
			
			nameConverter = new CamelCaseNameConverter();
		}
		
		return nameConverter;
	}
	
	private static String[] generateImpl(DatabaseProvider provider, PluggableNameConverter nameConverter, ClassLoader classloader, String... classes) throws ClassNotFoundException {
		List<String> back = new ArrayList<String>();
		Map<Class<? extends Entity>, Set<Class<? extends Entity>>> deps = 
			new HashMap<Class<? extends Entity>, Set<Class<? extends Entity>>>();
		Set<Class<? extends Entity>> roots = new LinkedHashSet<Class<? extends Entity>>();
		
		for (String cls : classes) {
			parseDependencies(deps, roots, (Class<? extends Entity>) Class.forName(cls, true, classloader));
		}
		
		while (!roots.isEmpty()) {
			Class<? extends Entity>[] rootsArray = roots.toArray(new Class[roots.size()]);
			roots.remove(rootsArray[0]);
			
			Class<? extends Entity> clazz = rootsArray[0];
			parseInterface(provider, nameConverter, clazz, back);
			
			List<Class<? extends Entity>> toRemove = new LinkedList<Class<? extends Entity>>();
			Iterator<Class<? extends Entity>> depIterator = deps.keySet().iterator();
			while (depIterator.hasNext()) {
				Class<? extends Entity> depClass = depIterator.next();
				
				Set<Class<? extends Entity>> individualDeps = deps.get(depClass);
				individualDeps.remove(clazz);
				
				if (individualDeps.isEmpty()) {
					roots.add(depClass);
					toRemove.add(depClass);
				}
			}
			
			for (Class<? extends Entity> remove : toRemove) {
				deps.remove(remove);
			}
		}
		
		return back.toArray(new String[back.size()]);
	}
	
	private static void parseDependencies(Map<Class <? extends Entity>, Set<Class<? extends Entity>>> deps, 
			Set<Class <? extends Entity>> roots, Class<? extends Entity>... classes) {
		for (Class<? extends Entity> clazz : classes) {
			if (deps.containsKey(clazz)) {
				continue;
			}
			
			Set<Class<? extends Entity>> individualDeps = new LinkedHashSet<Class<? extends Entity>>();
			
			for (Method method : clazz.getMethods()) {
				String attributeName = getAttributeNameFromMethod(method);
				Class<?> type = getAttributeTypeFromMethod(method);
				
				if (attributeName != null && type != null && interfaceInheritsFrom(type, Entity.class)) {
					if (!type.equals(clazz)) {
						individualDeps.add((Class<? extends Entity>) type);
					
						parseDependencies(deps, roots, (Class<? extends Entity>) type);
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
	
	private static void parseInterface(DatabaseProvider provider, PluggableNameConverter nameConverter, Class<? extends Entity> clazz, List<String> back) {
		StringBuilder sql = new StringBuilder();
		String sqlName = nameConverter.getName(clazz);
		
		DDLTable table = new DDLTable();
		table.setName(sqlName);
		
		table.setFields(parseFields(clazz));
		table.setForeignKeys(parseForeignKeys(nameConverter, clazz));
		
		sql.append(provider.render(table));
		
		back.add(sql.toString());
	}
	
	private static DDLField[] parseFields(Class<? extends Entity> clazz) {
		List<DDLField> fields = new ArrayList<DDLField>();
		List<String> attributes = new LinkedList<String>();
		
		DDLField field = new DDLField();
		
		field.setName("id");
		field.setAutoIncrement(true);
		field.setNotNull(true);
		field.setType(Types.INTEGER);
		field.setPrimaryKey(true);
		
		fields.add(field);
		
		for (Method method : clazz.getMethods()) {
			if (method.getAnnotation(Ignore.class) != null) {
				continue;
			}
			
			String attributeName = getAttributeNameFromMethod(method);
			Class<?> type = getAttributeTypeFromMethod(method);
			
			if (attributeName != null && type != null) {
				if (attributes.contains(attributeName)) {
					continue;
				}
				attributes.add(attributeName);
				
				int sqlType = -1;
				int precision = -1;
				int scale = -1;
				
				if (interfaceInheritsFrom(type, Entity.class)) {
					sqlType = Types.INTEGER;
				} else if (type.isArray()) {
					continue;
				} else {
					sqlType = SQLTypeEnum.getType(type).getSQLType();
					precision = SQLTypeEnum.getType(type).getPrecision();
				}
				
				SQLType sqlTypeAnnotation = method.getAnnotation(SQLType.class);
				if (sqlTypeAnnotation != null) {
					if (sqlTypeAnnotation.value() > 0) {
						sqlType = sqlTypeAnnotation.value();
					}
					precision = sqlTypeAnnotation.precision();
					scale = sqlTypeAnnotation.scale();
				}
				
				field = new DDLField();
				
				field.setName(attributeName);
				field.setType(sqlType);
				field.setPrecision(precision);
				field.setScale(scale);
				
				if (method.getAnnotation(PrimaryKey.class) != null) {
					field.setPrimaryKey(true);
				}
				
				if (method.getAnnotation(NotNull.class) != null) {
					field.setNotNull(true);
				}
				
				if (method.getAnnotation(AutoIncrement.class) != null) {
					field.setAutoIncrement(true);
				} else if (method.getAnnotation(Default.class) != null) {
					field.setDefaultValue(convertStringValue(method.getAnnotation(Default.class).value(), sqlType));
				}
				
				if (method.getAnnotation(OnUpdate.class) != null) {
					field.setOnUpdate(convertStringValue(method.getAnnotation(OnUpdate.class).value(), sqlType));
				}
				
				fields.add(field);
			}
		}
		
		return fields.toArray(new DDLField[fields.size()]);
	}
	
	private static DDLForeignKey[] parseForeignKeys(PluggableNameConverter nameConverter, Class<? extends Entity> clazz) {
		Set<DDLForeignKey> back = new LinkedHashSet<DDLForeignKey>();
		
		for (Method method : clazz.getMethods()) {
			String attributeName = getAttributeNameFromMethod(method);
			Class<?> type =  getAttributeTypeFromMethod(method);
			
			if (type != null && attributeName != null && interfaceInheritsFrom(type, Entity.class)) {
				DDLForeignKey key = new DDLForeignKey();
				
				key.setField(attributeName);
				key.setTable(nameConverter.getName((Class<? extends Entity>) type));
				key.setForeignField("id");
				
				back.add(key);
			}
		}
		
		for (Class<?> superInterface : clazz.getInterfaces()) {
			if (!superInterface.equals(Entity.class)) {
				back.addAll(Arrays.asList(parseForeignKeys(nameConverter, (Class<? extends Entity>) superInterface)));
			}
		}
		
		return back.toArray(new DDLForeignKey[back.size()]);
	}
	
	private static Object convertStringValue(String value, int type) {
		if (value == null) {
			return null;
		}
		
		DatabaseFunction func = DatabaseFunction.get(value.trim());
		if (func != null) {
			return func;
		}
		
		switch (type) {
			case Types.BIGINT:
				return Long.parseLong(value.trim());
				
			case Types.BIT:
				return Byte.parseByte(value.trim());
				
			case Types.BOOLEAN:
				return Boolean.parseBoolean(value.trim());
				
			case Types.CHAR:
				return value.charAt(0);
				
			case Types.DATE:
				return parseCalendar(value.trim());
				
			case Types.DECIMAL:
				return Double.parseDouble(value.trim());
				
			case Types.DOUBLE:
				return Double.parseDouble(value.trim());
				
			case Types.FLOAT:
				return Float.parseFloat(value.trim());
				
			case Types.INTEGER:
				return Integer.parseInt(value.trim());
				
			case Types.NUMERIC:
				return Integer.parseInt(value.trim());
				
			case Types.REAL:
				return Double.parseDouble(value.trim());
				
			case Types.SMALLINT:
				return Short.parseShort(value.trim());
				
			case Types.TIME:
				return parseCalendar(value.trim());
				
			case Types.TIMESTAMP:
				return parseCalendar(value.trim());
				
			case Types.TINYINT:
				return Short.parseShort(value.trim());
				
			case Types.VARCHAR:
				return "'" + value + "'";
		}
		
		return null;
	}
	
	private static Calendar parseCalendar(String ts) {
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(ts));
			
			return cal;
		} catch (java.text.ParseException e) {
			return Calendar.getInstance();
		}
	}
}
