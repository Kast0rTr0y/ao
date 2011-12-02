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
package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.EntityManager;
import net.java.ao.schema.StringLength;

/**
 * @author Daniel Spiewak
 */
public abstract class DatabaseType<T> {
	private final int type;
	private final String sqlTypeIdentifier;
	private final Class<?>[] handledTypes;

    protected DatabaseType(int type, String sqlTypeIdentifier, Class<?>... handledTypes) {
        this.type = type;
        this.sqlTypeIdentifier = sqlTypeIdentifier;
        this.handledTypes = handledTypes;
    }

    /**
     * The JDBC type constant from java.sql.Types.
     */
	public int getType() {
		return type;
	}

	/**
	 * What the type should be called in SQL statements.  This will vary by database dialect.
	 * It should include precision, length, or scale parameters if appropriate (e.g. "VARCHAR(255)").
	 */
    public String getSqlTypeIdentifier() {
        return sqlTypeIdentifier;
    }
    
    /**
     * Should this be considered the default type mapping for the given Java type?
     * (This method will become obsolete when we stop looking up type mappings by Java class only.)
     */
    public boolean isDefaultForJavaType() {
        return true;
    }
    
    /**
     * Should this be considered the default type mapping for the given JDBC type?
     * (This method will become obsolete when we stop looking up type mappings by Java class only.)
     */
    public boolean isDefaultForSqlType() {
        return true;
    }
    
	public Class<?>[] getHandledTypes() {
	    return handledTypes;
	}
	
	/**
	 * Returns a new DatabaseType instance with the same properties as this one except that it
	 * specifies the given maximum string length (which may be {@link StringLength#UNLIMITED}).
	 * @param length  a maximum string length greater than zero, or UNLIMITED
	 * @return  a DatabaseType instance
	 * @throws ActiveObjectsConfigurationException  if setting string length for this type is not allowed
	 */
	public DatabaseType<T> withStringLength(int length) {
	    throw new ActiveObjectsConfigurationException("@StringLength can only be specified for string properties");
	}

    /**
     * Returns a new DatabaseType instance with the same properties as this one except that it
     * specifies the field's numeric precision.
     * @param precision  the numeric precision; must be greater than zero
     * @return  a DatabaseType instance
     * @throws ActiveObjectsConfigurationException  if setting numeric precision for this type is not allowed
     */
    public DatabaseType<T> withPrecision(int length) {
        throw new ActiveObjectsConfigurationException("Precision can only be specified for numeric properties");
    }
    
    /**
     * Returns a new DatabaseType instance with the same properties as this one except that it
     * specifies the field's numeric scale.
     * @param precision  the numeric scale; must be greater than or equal to zero
     * @return  a DatabaseType instance
     * @throws ActiveObjectsConfigurationException  if setting numeric scale for this type is not allowed
     */
    public DatabaseType<T> withScale(int scale) {
        throw new ActiveObjectsConfigurationException("Scale can only be specified for numeric properties");
    }
    
	@SuppressWarnings("unchecked") 
	private boolean isSubclass(Class sup, Class sub) {
		if (sub.equals(sup)) {
			return true;
		} else if (sub.equals(Object.class)) {
			return false;
		}
		
		Class superclass = sub.getSuperclass();
		List<Class> superclasses = new LinkedList<Class>();
		superclasses.addAll(Arrays.asList(sub.getInterfaces()));
		
		if (superclass != null) {
			superclasses.add(superclass);
		}
		
		for (Class parent : superclasses) {
			if (isSubclass(sup, parent)) {
				return true;
			}
		}
		
		return false;
	}
	
	public boolean shouldCache(Class<?> type) {
		return true;
	}

    // validates the object before inserts
    public Object validate(Object o)
    {
        return o;
    }
	
	public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, T value) throws SQLException {
		stmt.setObject(index, value, getType());
	}
	
	public boolean valueEquals(Object val1, Object val2) {
		return val1.equals(val2);
	}
	
	public abstract T pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends T> type, String field) throws SQLException;
	
	public T pullFromDatabase(EntityManager manager, ResultSet res, Class<? extends T> type, int index) throws SQLException {
		return pullFromDatabase(manager, res, type, res.getMetaData().getColumnLabel(index));
	}
	
	public abstract Object defaultParseValue(String value);
	
	public String valueToString(Object value) {
		return value.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DatabaseType<?>) {
			DatabaseType<?> type = (DatabaseType<?>) obj;
			
			if (type.type == this.type && Arrays.equals(type.handledTypes, handledTypes)) {
				return true;
			}
		}
		
		return super.equals(obj);
	}
	
	@Override
	public int hashCode() {
		int hashCode = type;
		
		for (Class<?> type : handledTypes) {
			hashCode += type.hashCode();
		}
		hashCode %= 2 << 7;
		
		return hashCode;
	}
	
	@Override
	public String toString() {
	    return getClass().getSimpleName() + " (" + getSqlTypeIdentifier() + ")";
	}
}
