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
import java.util.LinkedHashSet;

public class IndexParser {

    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final TypeManager typeManager;

    public IndexParser(final NameConverters nameConverters, final TypeManager typeManager) {
        this.tableNameConverter = nameConverters.getTableNameConverter();
        this.fieldNameConverter = nameConverters.getFieldNameConverter();
        this.typeManager = typeManager;
    }

    public DDLIndex[] parseIndexes(Class<? extends RawEntity<?>> clazz) {
        final LinkedHashSet<DDLIndex> back = new LinkedHashSet<>();
        final String tableName = tableNameConverter.getName(clazz);

        for (Method method : clazz.getMethods()) {
            String attributeName = fieldNameConverter.getName(method);
            AnnotationDelegate annotations = Common.getAnnotationDelegate(fieldNameConverter, method);

            if (Common.isAccessor(method) || Common.isMutator(method)) {
                Indexed indexedAnno = annotations.getAnnotation(Indexed.class);
                Class<?> type = Common.getAttributeTypeFromMethod(method);

                if (indexedAnno != null || (type != null && RawEntity.class.isAssignableFrom(type))) {
                    DDLIndex index = new DDLIndex();
                    index.setField(attributeName);
                    index.setTable(tableName);
                    index.setType(SchemaGenerator.getSQLTypeFromMethod(typeManager, type, method, annotations));

                    back.add(index);
                }
            }
        }

        for (Class<?> superInterface : clazz.getInterfaces()) {
            if (RawEntity.class.isAssignableFrom(superInterface) &&
                    !(RawEntity.class.equals(superInterface) || superInterface.isAnnotationPresent(Polymorphic.class))) {
                back.addAll(Arrays.asList(parseIndexes((Class<? extends RawEntity<?>>) superInterface)));
            }
        }

        return back.toArray(new DDLIndex[back.size()]);
    }
}
