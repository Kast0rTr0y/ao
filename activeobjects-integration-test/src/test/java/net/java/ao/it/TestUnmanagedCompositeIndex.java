package net.java.ao.it;

import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.schema.Index;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.Indexes;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Data(TestUnmanagedCompositeIndex.EntityDatabaseUpdater.class)
public final class TestUnmanagedCompositeIndex extends ActiveObjectsIntegrationTest {

    private static final String UNMANAGED_INDEX_NAME = "index_ao_unmanaged";

    @Test
    public void testUnmanagedIndexShouldNotBeDroppedByAo() throws Exception {
        entityManager.migrate(TestCase.class);
        createUnmanagedIndex(UNMANAGED_INDEX_NAME);
        entityManager.migrate(TestCase.class);
        assertTrue(findIndexInDatabase(UNMANAGED_INDEX_NAME).isPresent());
    }

    private void createUnmanagedIndex(String indexName) throws Exception {
        DDLAction unmanagedIndexAction = new DDLAction(DDLActionType.CREATE_INDEX);
        DDLIndex index = DDLIndex.builder()
                .fields(
                        DDLIndexField.builder()
                                .fieldName(getFieldName(TestCase.class, "getFirst"))
                                .type(entityManager.getProvider().getTypeManager().getType(Integer.class))
                                .build(),
                        DDLIndexField.builder()
                                .fieldName(getFieldName(TestCase.class, "getSecond"))
                                .type(entityManager.getProvider().getTypeManager().getType(Integer.class))
                                .build()
                )
                .table(entityManager.getTableNameConverter().getName(TestCase.class))
                .indexName(indexName)
                .build();

        unmanagedIndexAction.setIndex(index);

        NameConverters unmanagedNameCoverters = mock(NameConverters.class);
        IndexNameConverter unmanagedIndexNameConverter = mock(IndexNameConverter.class);
        when(unmanagedIndexNameConverter.getName(anyString(), anyString())).thenReturn(indexName);
        when(unmanagedNameCoverters.getIndexNameConverter()).thenReturn(unmanagedIndexNameConverter);

        Iterable<SQLAction> createIndexActions = entityManager.getProvider().renderAction(unmanagedNameCoverters, unmanagedIndexAction);
        DbUtils.executeUpdate(entityManager, createIndexActions.iterator().next().getStatement(), mock(DbUtils.UpdateCallback.class));
    }

    private Optional<DDLIndex> findIndexInDatabase(final String indexName) throws SQLException {
        return Stream.of(getDdlTable().getIndexes())
                .filter(index -> index.getIndexName().equalsIgnoreCase(indexName))
                .findAny();
    }

    private DDLTable getDdlTable() throws SQLException {
        final DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), new DefaultSchemaConfiguration());
        final String tableName = entityManager.getNameConverters().getTableNameConverter().getName(TestCase.class);

        return Stream.of(tables)
                .filter(table -> table.getName().equalsIgnoreCase(tableName))
                .findAny()
                .orElseThrow(NoSuchElementException::new);
    }

    public static class EntityDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestCase.class);
        }
    }

    @Indexes({
            @Index(name = "first", methodNames = {"getFirst", "getAlwaysIndexed"}),
            @Index(name =  "second", methodNames = {"getSecond", "getAlwaysIndexed"}),
            @Index(name =  "third", methodNames = {"getThird", "getAlwaysIndexed"})
    })
    public interface TestCase extends Entity {

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