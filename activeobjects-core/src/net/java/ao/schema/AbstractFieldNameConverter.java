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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.java.ao.Common;
import net.java.ao.RawEntity;

import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Lists.*;

/**
 * An abstract implementation of {@link FieldNameConverter} which handles common
 * tasks for the name converter (i.e. relations annotations, accessor/mutator
 * annotations, etc).  For most tasks, custom field name converters should extend
 * this class, rather than directly implementing <code>FieldNameConverter</code>.
 *
 * @author Daniel Spiewak
 */
public abstract class AbstractFieldNameConverter implements FieldNameConverter
{
    private final List<FieldNameResolver> fieldNameResolvers;

    /**
     * Default constructor implementing the default behaviour for active objects.
     *
     * @see
     */
    protected AbstractFieldNameConverter()
    {
        this(Lists.<FieldNameResolver>newArrayList(
                new RelationalFieldNameResolver(),
                new MutatorFieldNameResolver(),
                new AccessorFieldNameResolver(),
                new PrimaryKeyFieldNameResolver(),
                new GetterFieldNameResolver(),
                new SetterFieldNameResolver(),
                new IsAFieldNameResolver(),
                new NullFieldNameResolver()
        ));
    }

    protected AbstractFieldNameConverter(List<FieldNameResolver> fieldNameResolvers)
    {
        this.fieldNameResolvers = checkNotNull(fieldNameResolvers);
    }

    /**
     * <p>Handles operations which should be common to all field name converters
     * such as overriding of the generated field name through annotations, etc.
     * This method also handles the converting through the Java Bean method
     * prefix convention (get/set/is), allowing the implementing class to only
     * concern itself with converting one <code>String</code> (from the method
     * name) into another.</p>
     *
     * <p>This method delegates the actual conversion logic to the
     * {@link #convertName(String)} method.  There is rarely a need
     * for subclasses to override this method.</p>
     *
     * @param method The method for which a field name must be generated.
     * @return A valid database identifier to be used as the field name representative
     *         of the method in question.
     * @see net.java.ao.schema.FieldNameConverter#getName(Method)
     */
    public final String getName(Method method)
    {
        return getNameInternal(method, PolyTypeHandler.STRAIGHT);
    }

    /**
     * Documentation on the {@link #getName(Method)} method.
     *
     * @return A valid database identifier to be used as the field name representative
     *         of the method in question.
     * @see net.java.ao.schema.FieldNameConverter#getPolyTypeName(Method)
     */
    public final String getPolyTypeName(Method method)
    {
        return getNameInternal(method, PolyTypeHandler.POLY);
    }

    private String getNameInternal(final Method method, final PolyTypeHandler polyTypeHandler)
    {
        final FieldNameResolver fieldNameResolver = findFieldNameResolver(checkNotNull(method));
        final String resolved = fieldNameResolver.resolve(method);

        if (resolved == null)
        {
            return null;
        }

        if (!fieldNameResolver.transform())
        {
            return resolved;
        }

        if (polyTypeHandler.equals(PolyTypeHandler.POLY))
        {
            return convertName(polyTypeHandler.handle(resolved));
        }
        else
        {
            final EntityFieldNameHandler from = EntityFieldNameHandler.from(method);
            return convertName(from.handle(polyTypeHandler.handle(resolved)));
        }
    }

    private static enum PolyTypeHandler
    {
        STRAIGHT, POLY
            {
                @Override
                String handle(String s)
                {
                    return s + "Type";
                }
            };

        String handle(String s)
        {
            return s;
        }
    }

    private static enum EntityFieldNameHandler
    {
        PRIMITIVE, ENTITY
            {
                @Override
                String handle(String s)
                {
                    return s + "ID";
                }
            };

        String handle(String s)
        {
            return s;
        }

        static EntityFieldNameHandler from(Method m)
        {
            return from(isAttributeOfTypeEntity(m));
        }

        static EntityFieldNameHandler from(boolean isEntity)
        {
            return isEntity ? ENTITY : PRIMITIVE;
        }
    }


    private FieldNameResolver findFieldNameResolver(final Method method)
    {
        return Iterables.find(fieldNameResolvers, new Predicate<FieldNameResolver>()
        {
            public boolean apply(FieldNameResolver input)
            {
                return input.accept(method);
            }
        });
    }

    private static boolean isAttributeOfTypeEntity(Method method)
    {
        return Common.interfaceInheritsFrom(Common.getAttributeTypeFromMethod(method), RawEntity.class);
    }

    protected abstract String convertName(String name);
}
