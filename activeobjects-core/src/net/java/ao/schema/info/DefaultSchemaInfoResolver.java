package net.java.ao.schema.info;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.java.ao.Common;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.PrimaryKey;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class DefaultSchemaInfoResolver implements SchemaInfoResolver
{

    @Override
    public <T extends RawEntity<?>> SchemaInfo<T> resolve(NameConverters nameConverters, Class<T> type)
    {
        FieldNameConverter fieldNameConverter = nameConverters.getFieldNameConverter();

        String primaryKeyFieldName = null;
        Set<Method> accessors = Sets.newHashSet();
        Set<Method> mutators = Sets.newHashSet();
        Map<Method, String> fieldNameByMethod = Maps.newHashMap();
        Map<String, String> polyNameByFieldName = Maps.newHashMap();
        Map<String, Class<?>> fieldTypeByFieldName = Maps.newHashMap();

        // First find all the methods
        final Set<Method> methods = Sets.newLinkedHashSet();
        visitDeclaredMethods(type, new Function<Method, Void>()
        {
            public Void apply(Method method)
            {
                methods.add(method);
                return null;
            }
        });

        for (Method method : methods)
        {
            String fieldName = null;
            if (Common.isAccessor(method))
            {
                fieldName = fieldNameConverter.getName(method);
                if (fieldName != null)
                {
                    fieldNameByMethod.put(method, fieldName);
                    accessors.add(method);
                }
            } else if (Common.isMutator(method))
            {
                fieldName = fieldNameConverter.getName(method);
                if (fieldName != null)
                {
                    fieldNameByMethod.put(method, fieldName);
                    mutators.add(method);
                }
            }

            if (fieldName != null && !fieldTypeByFieldName.containsKey(fieldName))
            {
                // figure out if there's a polymorphic annotation and keep track of the respective field name
                Class<?> fieldType = Common.getAttributeTypeFromMethod(method);
                // keep track of the field types, so we can use the db field types to convert the values
                fieldTypeByFieldName.put(fieldName, fieldType);

                // figure out if there's a polymorphic annotation and keep track of the respective field name
                String polyFieldName = fieldType.getAnnotation(Polymorphic.class) != null ? fieldNameConverter.getPolyTypeName(method) : null;
                if (polyFieldName != null)
                {
                    polyNameByFieldName.put(fieldName, polyFieldName);
                }
            }

            if (primaryKeyFieldName == null && method.isAnnotationPresent(PrimaryKey.class))
            {
                primaryKeyFieldName = fieldName;
            }

        }

        return new ImmutableSchemaInfo<T>(
                type,
                nameConverters.getTableNameConverter().getName(type),
                primaryKeyFieldName,
                accessors,
                mutators,
                fieldNameByMethod,
                polyNameByFieldName,
                fieldTypeByFieldName
        );
    }

    /**
     * Recursively read the interface hierarchy of the given AO type interface
     */
    static void visitDeclaredMethods(Class<?> baseType, final Function<Method, ?> visitor)
    {
        visitTypeHierarchy(baseType, new Function<Class<?>, Void>()
        {
            public Void apply(Class<?> type)
            {
                for (Method method : type.getDeclaredMethods())
                {
                    visitor.apply(method);
                }
                return null;
            }
        });
    }

    /**
     * Recursively read the interface hierarchy of the given AO type interface
     */
    static void visitTypeHierarchy(Class<?> type, Function<Class<?>, ?> visitor)
    {
        visitor.apply(type);
        for (Class<?> superType : type.getInterfaces())
        {
            visitTypeHierarchy(superType, visitor);
        }
    }

}
