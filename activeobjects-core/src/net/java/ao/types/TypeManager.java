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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;

import net.java.ao.Entity;

import net.java.ao.Common;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;

/**
 * <p>Central managing class for the ActiveObjects type system.  The type
 * system in AO is designed to allow extensibility and control over
 * how specific data types are handled internally.  All database-agnostic,
 * type-specific tasks are delegated to the actual type instances.  This
 * class acts as a singleton container for every available type, indexing
 * them based on corresponding Java type and JDBC integer type.</p>
 * 
 * <p>This container is thread safe and so may be used from within multiple
 * contexts.</p>
 * 
 * @author Daniel Spiewak
 * @see net.java.ao.types.DatabaseType
 */
public class TypeManager
{
    private final ImmutableMap<Class<?>, DatabaseType<?>> classIndex;
    private final ImmutableMap<Integer, DatabaseType<?>> intIndex;

    private TypeManager(Builder builder)
    {
        this.classIndex = ImmutableMap.copyOf(builder.classIndex);
        this.intIndex = ImmutableMap.copyOf(builder.intIndex);
    }
    
    public static class Builder
    {
        private final Map<Class<?>, DatabaseType<?>> classIndex = new HashMap<Class<?>, DatabaseType<?>>();
        private final Map<Integer, DatabaseType<?>> intIndex = new HashMap<Integer, DatabaseType<?>>();
        
        public Builder()
        {
            addMapping(new BigIntType());
            addMapping(new BooleanType());
            addMapping(new BlobType());
            addMapping(new DoubleType());
            addMapping(new FloatType());
            addMapping(new IntegerType());
            addMapping(new TimestampDateType());
            addMapping(new VarcharType());
            addMapping(new ClobType());
            addMapping(new EnumType());
            addMapping(new URLType());
            addMapping(new URIType());
        }
        
        public TypeManager build()
        {
            return new TypeManager(this);
        }
        
        public Builder addMapping(DatabaseType<?> typeInfo)
        {
            if (typeInfo.isDefaultForJavaType())
            {
                for (Class<?> clazz : typeInfo.getHandledTypes())
                {
                    classIndex.put(clazz, typeInfo);
                }
            }
            if (typeInfo.isDefaultForSqlType())
            {
                intIndex.put(typeInfo.getType(), typeInfo);
            }
            return this;
        }
    }
	
	/**
	 * <p>Returns the corresponding {@link DatabaseType} for a given Java
	 * class.  This is the primary mechanism used by ActiveObjects
	 * internally to obtain type instances.  Code external to the
	 * framework may also make use of this method to obtain the relevant
	 * type information or to just test if a type is in fact
	 * available.
	 * 
	 * @param javaType	The {@link Class} type for which a type instance
	 * 		should be returned.
	 * @return	The type instance which corresponds to the specified class.
	 * @throws	RuntimeException	If no type was found correspondant to the
	 * 		given class.
	 * @see #getType(int)
	 */
	@SuppressWarnings("unchecked")
	public <T> DatabaseType<T> getType(Class<T> javaType)
	{
	    if (RawEntity.class.isAssignableFrom(javaType))
	    {
	        return (DatabaseType<T>) new EntityType<Object>(this, (Class<? extends RawEntity<Object>>) javaType);
	    }
	    for (Class<?> clazz = javaType; clazz != null; clazz = clazz.getSuperclass())
	    {
	        DatabaseType<?> typeInfo = classIndex.get(clazz);
	        if (typeInfo != null)
	        {
	            return (DatabaseType<T>) typeInfo;
	        }
	    }
	    throw new RuntimeException("Unrecognized type: " + javaType.getName());
	}
	
	/**
	 * <p>Returns the corresponding {@link DatabaseType} for a given JDBC
	 * integer type.  Code external to the framework may also make use of 
	 * this method to obtain the relevant type information or to just test 
	 * if a type is in fact available.  Types are internally prioritized by 
	 * entry order.
	 * 
	 * @param sqlType	The JDBC {@link Types} constant for which a type
	 * 		instance should be retrieved.
	 * @return	The type instance which corresponds to the specified type constant.
	 * @throws	RuntimeException	If no type was found correspondant to the
	 * 		given type constant.
	 * @see #getType(Class)
	 */
	public DatabaseType<?> getType(int sqlType)
	{
	    DatabaseType<?> typeInfo = intIndex.get(sqlType);
	    return (typeInfo != null) ? typeInfo : new GenericType(sqlType);
	}
}
