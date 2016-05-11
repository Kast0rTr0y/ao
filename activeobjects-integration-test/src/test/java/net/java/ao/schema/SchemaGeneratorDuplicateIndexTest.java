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

public final class SchemaGeneratorDuplicateIndexTest extends ActiveObjectsIntegrationTest {

    @Test
    public void testShouldParseConflictingIndexes() {

        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(TestedEntity.class);

        List<DDLIndex> expectedIndexes = ImmutableList.<DDLIndex>builder()
                .add(DDLIndex.builder()
                        .field(field("getFirst", Integer.class, TestedEntity.class))
                        .table(tableName)
                        .indexName(indexName(tableName, getFieldName(TestedEntity.class, "getFirst")))
                        .build())
                .add(DDLIndex.builder()
                        .field(field("getFirst", Integer.class, TestedEntity.class))
                        .table(tableName)
                        .indexName(indexName(tableName, "ix1"))
                        .build())
                .add(DDLIndex.builder()
                        .field(field("getFirst", Integer.class, TestedEntity.class))
                        .table(tableName)
                        .indexName(indexName(tableName, "ix2"))
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
            @Index(name = "ix1", methodNames = {"getFirst"}),
            @Index(name = "ix2", methodNames = {"getFirst"})
    })
    public interface TestedEntity extends Entity {

        @Indexed
        int getFirst();

        void setFirst(int value);
    }
}
