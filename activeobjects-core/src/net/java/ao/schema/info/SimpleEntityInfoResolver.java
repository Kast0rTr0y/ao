package net.java.ao.schema.info;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.java.ao.AnnotationDelegate;
import net.java.ao.Common;
import net.java.ao.Generator;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.Transient;
import net.java.ao.schema.AutoIncrement;
import net.java.ao.schema.Default;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.Ignore;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.types.TypeManager;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * A {@code EntityInfoResolver} which creates new {@link EntityInfo} instances on every invocation of
 * {@link #resolve(Class)}
 */
public class SimpleEntityInfoResolver implements EntityInfoResolver
{

    private final NameConverters nameConverters;
    private final TypeManager typeManager;

    public SimpleEntityInfoResolver(NameConverters nameConverters, TypeManager typeManager) {
        this.nameConverters = nameConverters;
        this.typeManager = typeManager;
    }

    @Override
    public <T extends RawEntity<K>, K> EntityInfo<T, K> resolve(Class<T> type)
    {
        final FieldNameConverter fieldNameConverter = nameConverters.getFieldNameConverter();

        final Map<String, Method> accessorByFieldName = Maps.newHashMap();
        final Map<String, Method> mutatorByFieldName = Maps.newHashMap();

        // First find all the methods
        for (Method method : type.getMethods())
        {
            if (method.isAnnotationPresent(Ignore.class))
            {
                continue;
            }

            if (Common.isAccessor(method))
            {
                String name = fieldNameConverter.getName(method);
                if (name != null)
                {
                    if (accessorByFieldName.containsKey(name))
                    {
                        throw new IllegalArgumentException(String.format("Invalid Entity definition. Both %s and %s generate the same table name (%s)", method, accessorByFieldName.get(name), name));
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
                        throw new IllegalArgumentException(String.format("Invalid Entity definition. Both %s and %s generate the same table name (%s)", method, mutatorByFieldName.get(name), name));
                    }
                    mutatorByFieldName.put(fieldNameConverter.getName(method), method);
                }
            }
        }

        Set<FieldInfo> fields = Sets.newHashSet();

        for (String fieldName : Sets.union(accessorByFieldName.keySet(), mutatorByFieldName.keySet()))
        {
            fields.add(createFieldInfo(fieldName, accessorByFieldName.get(fieldName), mutatorByFieldName.get(fieldName)));
        }

        return new ImmutableEntityInfo<T, K>(
                type,
                nameConverters.getTableNameConverter().getName(type),
                fields
        );
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
                annotations.isAnnotationPresent(AutoIncrement.class),
                annotations.isAnnotationPresent(Default.class),
                generatorAnnotation != null ? generatorAnnotation.value() : null
        );
    }

    private AnnotationDelegate getAnnotations(Method accessor, Method mutator)
    {
        return accessor != null ? new AnnotationDelegate(accessor, mutator) : new AnnotationDelegate(mutator, accessor);
    }

}
