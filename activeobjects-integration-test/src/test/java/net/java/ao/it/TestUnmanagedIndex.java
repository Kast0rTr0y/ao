package net.java.ao.it;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.schema.IndexNameConverter;
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

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to recreate the issue described in:
 * https://ecosystem.atlassian.net/browse/AO-554?focusedCommentId=140467
 */
@Data(TestUnmanagedIndex.EntityDatabaseUpdater.class)
public final class TestUnmanagedIndex extends ActiveObjectsIntegrationTest {

    private static final String UNMANAGED_INDEX_NAME = "index_ao_unmanaged";

    @Test
    public void testUnmanagedIndexShouldNotBeDroppedByAo() throws Exception {
        entityManager.migrate(LexoRank.class);
        createUnmanagedIndex(UNMANAGED_INDEX_NAME);
        entityManager.migrate(LexoRank.class);
        assertTrue(findIndexInDatabase(UNMANAGED_INDEX_NAME).isPresent());
    }

    private void createUnmanagedIndex(String indexName) throws Exception {
        DDLAction unmanagedIndexAction = new DDLAction(DDLActionType.CREATE_INDEX);
        DDLIndex index = DDLIndex.builder()
                .field(DDLIndexField.builder()
                        .fieldName(entityManager.getFieldNameConverter().getName(LexoRank.class.getMethods()[0]))
                        .type(entityManager.getProvider().getTypeManager().getType(Integer.class))
                        .build()
                )
                .table(entityManager.getTableNameConverter().getName(LexoRank.class))
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
        final DDLTable table = getDdlTable();
        return Iterables.tryFind(newArrayList(table.getIndexes()), new Predicate<DDLIndex>() {
            @Override
            public boolean apply(DDLIndex index) {
                return index.getIndexName().equalsIgnoreCase(indexName);
            }
        });

    }

    private DDLTable getDdlTable() throws SQLException {
        final DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), new DefaultSchemaConfiguration());
        return Iterables.find(newArrayList(tables), new Predicate<DDLTable>() {
            @Override
            public boolean apply(DDLTable t) {
                return t.getName().equalsIgnoreCase(entityManager.getNameConverters().getTableNameConverter().getName(LexoRank.class));
            }
        });
    }

    public static class EntityDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(LexoRank.class);
        }
    }

    public static interface LexoRank extends Entity {
        int getRank();

        void setRank(int rank);

        String getType();

        void setType(String type);
    }

}