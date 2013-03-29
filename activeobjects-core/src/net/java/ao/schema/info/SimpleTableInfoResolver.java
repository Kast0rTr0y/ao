package net.java.ao.schema.info;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.java.ao.*;
import net.java.ao.schema.*;
import net.java.ao.types.TypeManager;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

public class SimpleTableInfoResolver implements TableInfoResolver
{

    private final NameConverters nameConverters;
    private final TypeManager typeManager;

    public SimpleTableInfoResolver(NameConverters nameConverters, TypeManager typeManager) {
        this.nameConverters = nameConverters;
        this.typeManager = typeManager;
    }

    @Override
    public <T extends RawEntity<K>, K> TableInfo<T, K> resolve(Class<T> type)
    {
        final FieldNameConverter fieldNameConverter = nameConverters.getFieldNameConverter();

        final Map<String, Method> accessorByFieldName = Maps.newHashMap();
        final Map<String, Method> mutatorByFieldName = Maps.newHashMap();

        // First find all the methods
        visitDeclaredMethods(type, new Function<Method, Void>()
        {
            public Void apply(Method method)
            {
                if (method.isAnnotationPresent(Ignore.class))
                {
                    return null;
                }

                if (Common.isAccessor(method))
                {
                    String name = fieldNameConverter.getName(method);
                    if (name != null)
                    {
                        if (accessorByFieldName.containsKey(name))
                        {
                            method = preferredMethod(method, accessorByFieldName.get(name));
                        }
                        accessorByFieldName.put(name, method);
                    }
                }
                else if (Common.isMutator(method))
                {
                    String name = fieldNameConverter.getName(method);
                    if (name != null)
                    {
                        if (mutatorByFieldName.containsKey(name))
                        {
                            method = preferredMethod(method, mutatorByFieldName.get(name));
                        }
                        mutatorByFieldName.put(fieldNameConverter.getName(method), method);
                    }
                }
                return null;
            }
        });

        Set<FieldInfo> fields = Sets.newHashSet();

        for (String fieldName : Sets.union(accessorByFieldName.keySet(), mutatorByFieldName.keySet()))
        {
            fields.add(createFieldInfo(fieldName, accessorByFieldName.get(fieldName), mutatorByFieldName.get(fieldName)));
        }

        return new ImmutableTableInfo<T, K>(
                type,
                nameConverters.getTableNameConverter().getName(type),
                fields
        );
    }

    private Method preferredMethod(Method methodA, Method methodB)
    {
        return methodA.getDeclaringClass().isAssignableFrom(methodB.getDeclaringClass()) ?
                methodB :
                methodA;
    }

    @SuppressWarnings("unchecked")
    private FieldInfo createFieldInfo(String fieldName, Method accessor, Method mutator)
    {
        Class fieldType = Common.getAttributeTypeFromMethod(Objects.firstNonNull(accessor, mutator));

        AnnotationDelegate annotations = getAnnotations(accessor, mutator);

        Generator generatorAnnotation = annotations.getAnnotation(Generator.class);
        return new ImmutableFieldInfo(
                fieldName,
                fieldType.isAnnotationPresent(Polymorphic.class) ? nameConverters.getFieldNameConverter().getPolyTypeName(Objects.firstNonNull(accessor, mutator)) : null,
                accessor,
                mutator,
                fieldType,
                typeManager.getType(fieldType),
                annotations.isAnnotationPresent(PrimaryKey.class),
                !annotations.isAnnotationPresent(NotNull.class),
                annotations.isAnnotationPresent(Transient.class),
                false,
                annotations.isAnnotationPresent(AutoIncrement.class),
                annotations.isAnnotationPresent(Default.class),
                generatorAnnotation != null ? generatorAnnotation.value() : null
        );
    }

    private AnnotationDelegate getAnnotations(Method accessor, Method mutator)
    {
        return accessor != null ? new AnnotationDelegate(accessor, mutator) : new AnnotationDelegate(mutator, accessor);
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
