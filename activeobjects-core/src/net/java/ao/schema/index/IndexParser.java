package net.java.ao.schema.index;

import net.java.ao.AnnotationDelegate;
import net.java.ao.Common;
import net.java.ao.DatabaseProvider;
import net.java.ao.Polymorphic;
import net.java.ao.RawEntity;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.Index;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.Indexes;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SchemaGenerator;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.types.TypeManager;
import net.java.ao.util.StreamUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexParser {

    private final DatabaseProvider databaseProvider;
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final IndexNameConverter indexNameConverter;
    private final TypeManager typeManager;

    private Predicate<Class<?>> extendsRawEntity = RawEntity.class::isAssignableFrom;
    private Predicate<Class<?>> isPolymorphic = clazz -> clazz.isAnnotationPresent(Polymorphic.class);
    private Predicate<Class<?>> isRawEntity = RawEntity.class::equals;

    public IndexParser(
            final DatabaseProvider databaseProvider,
            final NameConverters nameConverters,
            final TypeManager typeManager
    ) {
        this.databaseProvider = databaseProvider;
        this.tableNameConverter = nameConverters.getTableNameConverter();
        this.fieldNameConverter = nameConverters.getFieldNameConverter();
        this.indexNameConverter = nameConverters.getIndexNameConverter();
        this.typeManager = typeManager;
    }

    public Set<DDLIndex> parseIndexes(Class<? extends RawEntity<?>> clazz) {
        final String tableName = tableNameConverter.getName(clazz);

        final Stream<DDLIndex> methodIndexes = Arrays.stream(clazz.getMethods())
                .filter(Common::isMutatorOrAccessor)
                .filter(method -> isIndexed(method) || attributeExtendsRawEntity(method))
                .map(this::parseIndexField)
                .map(indexField -> createIndex(indexField, tableName));

        final Stream<DDLIndex> classIndexes = StreamUtils.ofNullable(clazz.getAnnotation(Indexes.class))
                .map(Indexes::value)
                .flatMap(Stream::of)
                .map(index -> parseCompositeIndex(index, tableName, clazz));

        final Stream<DDLIndex> superInterfaceIndexes = Arrays.stream(clazz.getInterfaces())
                .filter(extendsRawEntity)
                .filter(isRawEntity.negate())
                .filter(isPolymorphic.negate())
                .map(superInterface -> parseIndexes((Class<? extends RawEntity<?>>) superInterface))
                .flatMap(Set::stream);

        return Stream.of(methodIndexes, classIndexes, superInterfaceIndexes)
                .flatMap(Function.identity())
                .filter(this::hasFields)
                .filter(this::hasOnlyValidFields)
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

    @Nullable
    private DDLIndexField parseIndexField(@Nullable Method method) {
        if (method == null) {
            return null;
        }

        Class<?> type = Common.getAttributeTypeFromMethod(method);

        String attributeName = fieldNameConverter.getName(method);
        AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldNameConverter, method);

        return DDLIndexField.builder()
                .fieldName(attributeName)
                .type(SchemaGenerator.getSQLTypeFromMethod(typeManager, type, method, annotations))
                .build();
    }

    private DDLIndex createIndex(DDLIndexField indexField, String tableName) {
        return createIndex(Stream.of(indexField), tableName, computeIndexName(tableName, indexField.getFieldName()));
    }

    private DDLIndex createIndex(Stream<DDLIndexField> indexFields, String tableName, String indexName) {
        return DDLIndex.builder()
                .indexName(indexName)
                .table(tableName)
                .fields(indexFields.toArray(DDLIndexField[]::new))
                .build();
    }

    private DDLIndex parseCompositeIndex(Index index, String tableName, Class<? extends RawEntity<?>> clazz) {

        final Map<String, Method> methodsApplicableForIndexing = Stream.of(clazz.getMethods())
                .filter(Common::isMutatorOrAccessor)
                .collect(Collectors.toMap(Method::getName, Function.identity()));

        final Stream<DDLIndexField> indexFields = Stream.of(index.methodNames())
                .map(methodsApplicableForIndexing::get)
                .map(this::parseIndexField);

        final String indexName = computeIndexName(tableName, index.name());

        return createIndex(indexFields, tableName, indexName);
    }

    private String computeIndexName(String tableName, String indexName) {
        return indexNameConverter.getName(databaseProvider.shorten(tableName), databaseProvider.shorten(indexName));
    }

    private boolean hasFields(DDLIndex index) {
        return index.getFields().length > 0;
    }

    private boolean hasOnlyValidFields(DDLIndex index) {
        return Stream.of(index.getFields())
                .allMatch(Objects::nonNull);
    }
}
