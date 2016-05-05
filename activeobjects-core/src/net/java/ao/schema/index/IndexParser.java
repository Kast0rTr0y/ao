package net.java.ao.schema.index;

import net.java.ao.AnnotationDelegate;
import net.java.ao.Common;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.types.TypeManager;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexParser {

    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final TypeManager typeManager;

    private Predicate<Class<?>> extendsRawEntity = RawEntity.class::isAssignableFrom;
    private Predicate<Class<?>> isPolymorphic = clazz -> clazz.isAnnotationPresent(Polymorphic.class);
    private Predicate<Class<?>> isRawEntity = RawEntity.class::equals;

    public IndexParser(final NameConverters nameConverters, final TypeManager typeManager) {
        this.tableNameConverter = nameConverters.getTableNameConverter();
        this.fieldNameConverter = nameConverters.getFieldNameConverter();
        this.typeManager = typeManager;
    }

    public Set<DDLIndex> parseIndexes(Class<? extends RawEntity<?>> clazz) {
        final String tableName = tableNameConverter.getName(clazz);

        final Stream<DDLIndex> methodIndexes = Arrays.stream(clazz.getMethods())
                .filter(Common::isMutatorOrAccessor)
                .filter(method -> isIndexed(method) || attributeExtendsRawEntity(method))
                .map(this::parseIndexField)
                .map(indexField -> createIndex(indexField, tableName));

        final Stream<DDLIndex> superInterfaceIndexes = Arrays.stream(clazz.getInterfaces())
                .filter(extendsRawEntity)
                .filter(isRawEntity.negate())
                .filter(isPolymorphic.negate())
                .map(superInterface -> parseIndexes((Class<? extends RawEntity<?>>) superInterface))
                .flatMap(Set::stream);

        return Stream.concat(methodIndexes, superInterfaceIndexes)
                .collect(Collectors.toSet());
    }

    private boolean isIndexed(Method method) {
        final AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldNameConverter, method);
        final Indexed indexed = annotations.getAnnotation(Indexed.class);

        return indexed != null;
    }

    private boolean attributeExtendsRawEntity(Method method) {
        Class<?> type = Common.getAttributeTypeFromMethod(method);

        return type != null && extendsRawEntity.test(type);
    }

    private DDLIndexField parseIndexField(Method method) {
        Class<?> type = Common.getAttributeTypeFromMethod(method);

        String attributeName = fieldNameConverter.getName(method);
        AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldNameConverter, method);

        return DDLIndexField.builder()
                .fieldName(attributeName)
                .type(SchemaGenerator.getSQLTypeFromMethod(typeManager, type, method, annotations))
                .build();
    }

    private DDLIndex createIndex(DDLIndexField indexField, String tableName) {
        final DDLIndex index = new DDLIndex();
        index.setFields(new DDLIndexField[]{indexField});
        index.setTable(tableName);

        return index;
    }
}
