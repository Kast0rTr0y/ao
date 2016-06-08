package net.java.ao.schema;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.helper.DatabaseMetaDataReader;
import net.java.ao.schema.helper.DatabaseMetaDataReaderImpl;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static net.java.ao.matcher.IndexMatchers.index;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;

public final class DuplicateIndexDeclarationTest extends ActiveObjectsIntegrationTest {
    private DatabaseMetaDataReader reader;

    @Before
    public void setUp() {
        SchemaConfiguration schemaConfiguration = getSchemaConfiguration();
        reader = new DatabaseMetaDataReaderImpl(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
    }

    @Test
    public void shouldIgnoreDuplicateIndexesWhenParsing() throws Exception{
        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(DuplicateIndexes.T.class);

        DDLTable[] parsedTables = SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(), DuplicateIndexes.T.class);
        DDLTable actualTable = Stream.of(parsedTables)
                .filter(table -> table.getName().equals(tableName))
                .findAny()
                .orElse(null);

        assertThat(actualTable.getIndexes(), arrayWithSize(1));
    }

    @Test
    public void shouldIgnoreDuplicateIndexesWhenCreating() throws Exception{
        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(DuplicateIndexes.T.class);
        final String firstFieldName = getFieldName(DuplicateIndexes.T.class, "getFirst");

        entityManager.migrate(Clean.T.class);
        entityManager.migrate(DuplicateIndexes.T.class);
        with(connection -> {
            final Iterable<? extends net.java.ao.schema.helper.Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);

            assertThat(indexes, iterableWithSize(1));
            assertThat(indexes, contains(index(tableName, ImmutableList.of(firstFieldName))));
        });
    }

    @Test
    public void shouldIgnoreDuplicateIndexesWhenMigrating() throws Exception{
        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(DuplicateIndexes.T.class);
        final String firstFieldName = getFieldName(DuplicateIndexes.T.class, "getFirst");

        entityManager.migrate(Clean.T.class);
        entityManager.migrate(SingleIndex.T.class);
        entityManager.migrate(DuplicateIndexes.T.class);
        with(connection -> {
            final Iterable<? extends net.java.ao.schema.helper.Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);

            assertThat(indexes, iterableWithSize(1));
            assertThat(indexes, contains(index(tableName, ImmutableList.of(firstFieldName))));
        });
    }

    static class Clean {
        public interface T extends Entity {
        }
    }

    static class SingleIndex {
        public interface T extends Entity {

            @Indexed
            int getFirst();
            void setFirst(int value);
        }
    }

    static class DuplicateIndexes {
        @Indexes({
                @Index(name = "ix1", methodNames = {"getFirst"}),
                @Index(name = "ix2", methodNames = {"getFirst"})
        })
        public interface T extends Entity {

            @Indexed
            int getFirst();
            void setFirst(int value);
        }
    }
}
