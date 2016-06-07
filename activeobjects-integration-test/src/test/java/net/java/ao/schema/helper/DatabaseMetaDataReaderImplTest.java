package net.java.ao.schema.helper;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.Indexed;
import net.java.ao.schema.Indexes;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.contains;
import static net.java.ao.matcher.IndexMatchers.index;
import static net.java.ao.matcher.IndexMatchers.isNamed;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Data(DatabaseMetaDataReaderImplTest.DatabaseMetadataReaderImplTestUpdater.class)
public final class DatabaseMetaDataReaderImplTest extends ActiveObjectsIntegrationTest {
    private DatabaseMetaDataReader reader;

    @Before
    public void setUp() {
        SchemaConfiguration schemaConfiguration = getSchemaConfiguration();
        reader = new DatabaseMetaDataReaderImpl(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
    }

    @NonTransactional
    @Test
    public void shouldGetMultipleCompositeIndexes() throws Exception {
        final String tableName = getTableName(MultipleComposite.class, false);
        final String firstField = getFieldName(MultipleComposite.class, "getFirst");
        final String secondField = getFieldName(MultipleComposite.class, "getSecond");
        final String thirdField = getFieldName(MultipleComposite.class, "getThird");
        final String otherField = getFieldName(MultipleComposite.class, "getOther");


        with(connection -> {
            final Iterable<? extends Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);

            assertThat(indexes, containsInAnyOrder(
                    index(tableName, ImmutableList.of(firstField, otherField)),
                    index(tableName, ImmutableList.of(secondField, otherField)),
                    index(tableName, ImmutableList.of(thirdField, otherField)),
                    index(tableName, ImmutableList.of(otherField))
            ));
        });
    }

    @Test
    public void testGetTableNames() throws Exception {
        with(new WithConnection() {
            @Override
            public void call(Connection connection) throws Exception {
                final Iterable<String> tableNames = reader.getTableNames(connection.getMetaData());

                assertEquals(3, Iterables.size(tableNames));
                assertTrue(containsTableName(tableNames, getTableName(Simple.class, false)));
                assertTrue(containsTableName(tableNames, getTableName(Other.class, false)));
                assertTrue(containsTableName(tableNames, getTableName(MultipleComposite.class, false)));
            }
        });
    }

    private boolean containsTableName(Iterable<String> tableNames, final String tableName) {
        return any(tableNames, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.equalsIgnoreCase(tableName);
            }
        });
    }

    @Test
    public void testGetFields() throws Exception {
        final String tableName = getTableName(Simple.class, false);

        with(new WithConnection() {
            @Override
            public void call(Connection connection) throws Exception {
                Iterable<? extends net.java.ao.schema.helper.Field> fields = reader.getFields(connection.getMetaData(), tableName);

                assertEquals(3, Iterables.size(fields));
                final Iterable<String> fieldNames = Iterables.transform(fields, new Function<net.java.ao.schema.helper.Field, String>() {
                    @Override
                    public String apply(net.java.ao.schema.helper.Field from) {
                        return from.getName();
                    }
                });
                assertTrue(contains(fieldNames, getFieldName(Simple.class, "getID")));
                assertTrue(contains(fieldNames, getFieldName(Simple.class, "getName")));
                assertTrue(contains(fieldNames, getFieldName(Simple.class, "getOther")));
            }
        });
    }

    @Test
    public void testGetIndexes() throws Exception {
        final String tableName = getTableName(Simple.class, false);

        with(connection -> {
            final Iterable<Index> indexes = (Iterable<Index>) reader.getIndexes(connection.getMetaData(), tableName);
            assertThat(indexes, hasItem(index(tableName, getFieldName(Simple.class, "getName"))));
            assertThat(indexes, everyItem(isNamed()));
        });
    }

    @Test
    public void testGetForeignKeys() throws Exception {
        final String tableName = getTableName(Simple.class, false);
        with(new WithConnection() {
            @Override
            public void call(Connection connection) throws Exception {
                final Iterable<? extends ForeignKey> foreignKeys = reader.getForeignKeys(connection.getMetaData(), tableName);
                assertEquals(1, Iterables.size(foreignKeys));
                containsForeignKey(foreignKeys,
                        tableName, getFieldName(Simple.class, "getOther"),
                        getTableName(Other.class, false), getFieldName(Other.class, "getID"));
            }
        });
    }

    private void containsForeignKey(Iterable<? extends ForeignKey> foreignKeys, final String localTable, final String localColumn, final String foreignTable, final String foreignColumn) {
        any(foreignKeys, new Predicate<ForeignKey>() {
            @Override
            public boolean apply(ForeignKey fk) {
                return fk.getLocalTableName().equalsIgnoreCase(localTable)
                        && fk.getLocalFieldName().equalsIgnoreCase(localColumn)
                        && fk.getForeignTableName().equalsIgnoreCase(foreignTable)
                        && fk.getForeignFieldName().equalsIgnoreCase(foreignColumn);
            }
        });
    }

    private void with(WithConnection w) throws Exception {
        Connection connection = null;
        try {
            connection = entityManager.getProvider().getConnection();
            w.call(connection);
        } finally {
            closeQuietly(connection);
        }
    }

    private static interface WithConnection {
        void call(Connection connection) throws Exception;
    }


    @Indexes({
            @net.java.ao.schema.Index(name = "first", methodNames = {"getFirst", "getOther"}),
            @net.java.ao.schema.Index(name = "second", methodNames = {"getSecond", "getOther"}),
            @net.java.ao.schema.Index(name = "third", methodNames = {"getThird", "getOther"}),
            @net.java.ao.schema.Index(name = "single", methodNames = {"getOther"})
    })
    public interface MultipleComposite extends Entity {

        String getFirst();

        void setFirst(String first);

        String getSecond();

        void setSecond(String second);

        String getThird();

        void setThird(String third);

        String getOther();

        void setOther(String other);
    }

    public static interface Simple extends Entity {
        @Indexed
        public String getName();

        public void setName(String id);

        public Other getOther();

        public void setOther(Other o);
    }

    public static interface Other extends Entity {
        @Indexed
        public String getName();

        public void setName(String id);
    }

    public static final class DatabaseMetadataReaderImplTestUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(Simple.class, MultipleComposite.class);
        }
    }
}
