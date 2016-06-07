package net.java.ao;

import com.google.common.collect.ImmutableList;
import net.java.ao.schema.Indexes;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.helper.DatabaseMetaDataReader;
import net.java.ao.schema.helper.DatabaseMetaDataReaderImpl;
import net.java.ao.schema.helper.Index;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Before;
import org.junit.Test;

import static net.java.ao.matcher.IndexMatchers.index;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsIterableWithSize.iterableWithSize;

public final class IndexFieldOrderSensitivityTest extends ActiveObjectsIntegrationTest {
    private DatabaseMetaDataReader reader;

    @Before
    public void setUp() {
        SchemaConfiguration schemaConfiguration = getSchemaConfiguration();
        reader = new DatabaseMetaDataReaderImpl(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
    }

    @NonTransactional
    @Test
    public void shouldChangeIndexFieldOrder() throws Exception {
        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(Clean.T.class);
        final String nameFieldName = getFieldName(WithIndex.T.class, "getName");
        final String secondNameFieldName = getFieldName(WithIndex.T.class, "getSecondName");

        entityManager.migrate(Clean.T.class);

        with(connection -> {
            final Iterable<? extends Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);
            assertThat(indexes, iterableWithSize(0));
        });

        entityManager.migrate(WithIndex.T.class);
        with(connection -> {
            final Iterable<? extends Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);

            assertThat(indexes, iterableWithSize(1));
            assertThat(indexes, contains(index(tableName, ImmutableList.of(nameFieldName, secondNameFieldName))));
        });

        entityManager.migrate(WithIndexInDifferentOrder.T.class);
        with(connection -> {
            final Iterable<? extends Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);

            assertThat(indexes, iterableWithSize(1));
            assertThat(indexes, contains(index(tableName, ImmutableList.of(secondNameFieldName, nameFieldName))));
        });
    }

    static class Clean {
        public interface T extends Entity {
        }
    }

    static class WithIndex {
        @Indexes({
                @net.java.ao.schema.Index(name = "idx", methodNames = {"getName", "getSecondName"})
        })
        public interface T extends Entity {
            String getName();
            void setName(String name);
            String getSecondName();
            void setSecondName(String secondName);
        }
    }

    static class WithIndexInDifferentOrder {
        @Indexes({
                @net.java.ao.schema.Index(name = "idx", methodNames = {"getSecondName", "getName"})
        })
        public interface T extends Entity {
            String getName();
            void setName(String name);
            String getSecondName();
            void setSecondName(String secondName);
        }
    }
}
