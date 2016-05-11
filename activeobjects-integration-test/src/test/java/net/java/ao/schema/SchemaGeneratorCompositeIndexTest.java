package net.java.ao.schema;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public final class SchemaGeneratorCompositeIndexTest extends ActiveObjectsIntegrationTest {

    @Test
    public void testShouldParseCompositeIndexes() {

        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(TestedEntity.class);

        List<DDLIndex> expectedIndexes = ImmutableList.<DDLIndex>builder()
                .add(DDLIndex.builder()
                        .field(field("getFirst", Integer.class, TestedEntity.class))
                        .table(tableName)
                        .indexName(indexName(tableName, getFieldName(TestedEntity.class, "getFirst")))
                        .build())
                .add(DDLIndex.builder()
                        .fields(
                                field("getFirst", Integer.class, TestedEntity.class),
                                field("getAlwaysIndexed", Integer.class, TestedEntity.class)
                        )
                        .indexName(indexName(tableName, "first"))
                        .table(tableName)
                        .build())
                .add(DDLIndex.builder()
                        .fields(
                                field("getSecond", Integer.class, TestedEntity.class),
                                field("getAlwaysIndexed", Integer.class, TestedEntity.class)
                        )
                        .indexName(indexName(tableName, "second"))
                        .table(tableName)
                        .build())
                .add(DDLIndex.builder()
                        .fields(
                                field("getThird", Integer.class, TestedEntity.class),
                                field("getAlwaysIndexed", Integer.class, TestedEntity.class)
                        )
                        .indexName(indexName(tableName, "third"))
                        .table(tableName)
                        .build())
                .build();

        DDLTable[] parsedTables = SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(), TestedEntity.class);
        DDLTable actualTable = Stream.of(parsedTables)
                .filter(table -> table.getName().equals(tableName))
                .findAny()
                .orElse(null);

        assertEquals(expectedIndexes.size(), actualTable.getIndexes().length);
        assertThat(Arrays.asList(actualTable.getIndexes()), containsInAnyOrder(expectedIndexes.toArray()));
    }

    @Indexes({
            @Index(name = "first", methodNames = {"getFirst", "getAlwaysIndexed"}),
            @Index(name =  "second", methodNames = {"getSecond", "getAlwaysIndexed"}),
            @Index(name =  "third", methodNames = {"getThird", "getAlwaysIndexed"})
    })
    public interface TestedEntity extends Entity {

        @Indexed
        int getFirst();

        int getSecond();

        int getThird();

        int getAlwaysIndexed();

        void setFirst(int value);

        void setSecond(int value);

        void setThird(int value);

        void setAlwaysIndexed(int value);

    }
}
