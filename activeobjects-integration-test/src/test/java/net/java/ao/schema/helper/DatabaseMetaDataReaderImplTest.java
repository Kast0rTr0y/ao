package net.java.ao.schema.helper;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.Indexed;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;

import static com.google.common.collect.Iterables.*;
import static net.java.ao.sql.SqlUtils.closeQuietly;
import static org.junit.Assert.*;

@Data(DatabaseMetaDataReaderImplTest.DatabaseMetadataReaderImplTestUpdater.class)
public final class DatabaseMetaDataReaderImplTest extends ActiveObjectsIntegrationTest
{
    private DatabaseMetaDataReader reader;

    @Before
    public void setUp()
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        reader = new DatabaseMetaDataReaderImpl(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
    }

    @Test
    public void testGetTableNames() throws Exception
    {
        with(new WithConnection()
        {
            @Override
            public void call(Connection connection) throws Exception
            {
                final Iterable<String> tableNames = reader.getTableNames(connection.getMetaData());

                assertEquals(2, Iterables.size(tableNames));
                assertTrue(containsTableName(tableNames, getTableName(Simple.class, false)));
                assertTrue(containsTableName(tableNames, getTableName(Other.class, false)));
            }
        });
    }

    private boolean containsTableName(Iterable<String> tableNames, final String tableName)
    {
        return any(tableNames, new Predicate<String>()
        {
            @Override
            public boolean apply(String input)
            {
                return input.equalsIgnoreCase(tableName);
            }
        });
    }

    @Test
    public void testGetFields() throws Exception
    {
        final String tableName = getTableName(Simple.class, false);

        with(new WithConnection()
        {
            @Override
            public void call(Connection connection) throws Exception
            {
                Iterable<? extends net.java.ao.schema.helper.Field> fields = reader.getFields(connection.getMetaData(), tableName);

                assertEquals(3, Iterables.size(fields));
                final Iterable<String> fieldNames = Iterables.transform(fields, new Function<net.java.ao.schema.helper.Field, String>()
                {
                    @Override
                    public String apply(net.java.ao.schema.helper.Field from)
                    {
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
    public void testGetIndexes() throws Exception
    {
        final String tableName = getTableName(Simple.class, false);

        with(new WithConnection()
        {
            @Override
            public void call(Connection connection) throws Exception
            {
                final Iterable<? extends Index> indexes = reader.getIndexes(connection.getMetaData(), tableName);
                assertTrue(containsIndex(indexes, tableName, getFieldName(Simple.class, "getName")));
            }
        });
    }

    private boolean containsIndex(Iterable<? extends Index> indexes, final String tableName, final String columnName)
    {
        return Iterables.any(indexes, new Predicate<Index>()
        {
            @Override
            public boolean apply(Index i)
            {
                return i.getTableName().equals(tableName) && i.getFieldName().equals(columnName);
            }
        });
    }

    @Test
    public void testGetForeignKeys() throws Exception
    {
        final String tableName = getTableName(Simple.class, false);
        with(new WithConnection()
        {
            @Override
            public void call(Connection connection) throws Exception
            {
                final Iterable<? extends ForeignKey> foreignKeys = reader.getForeignKeys(connection.getMetaData(), tableName);
                assertEquals(1, Iterables.size(foreignKeys));
                containsForeignKey(foreignKeys,
                        tableName, getFieldName(Simple.class, "getOther"),
                        getTableName(Other.class, false), getFieldName(Other.class, "getID"));
            }
        });
    }

    private void containsForeignKey(Iterable<? extends ForeignKey> foreignKeys, final String localTable, final String localColumn, final String foreignTable, final String foreignColumn)
    {
        any(foreignKeys, new Predicate<ForeignKey>()
        {
            @Override
            public boolean apply(ForeignKey fk)
            {
                return fk.getLocalTableName().equalsIgnoreCase(localTable)
                        && fk.getLocalFieldName().equalsIgnoreCase(localColumn)
                        && fk.getForeignTableName().equalsIgnoreCase(foreignTable)
                        && fk.getForeignFieldName().equalsIgnoreCase(foreignColumn);
            }
        });
    }

    private void with(WithConnection w) throws Exception
    {
        Connection connection = null;
        try
        {
            connection = entityManager.getProvider().getConnection();
            w.call(connection);
        }
        finally
        {
            closeQuietly(connection);
        }
    }

    private static interface WithConnection
    {
        void call(Connection connection) throws Exception;
    }


    public static interface Simple extends Entity
    {
        @Indexed
        public String getName();

        public void setName(String id);

        public Other getOther();

        public void setOther(Other o);
    }

    public static interface Other extends Entity
    {
        @Indexed
        public String getName();

        public void setName(String id);
    }

    public static final class DatabaseMetadataReaderImplTestUpdater implements DatabaseUpdater
    {
        @Override
        public void update(EntityManager entityManager) throws Exception
        {
            entityManager.migrate(Simple.class);
        }
    }

    public static Object getFieldValue(Object target, String name)
    {
        try
        {
            java.lang.reflect.Field field = findField(name, target.getClass());
            field.setAccessible(true);
            return field.get(target);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Field findField(String name, Class<?> targetClass)
    {
        return findField(name, targetClass, null);
    }

    public static Field findField(String name, Class<?> targetClass, Class<?> type)
    {
        Class<?> search = targetClass;
        while (!Object.class.equals(search) && search != null)
        {
            for (Field field : search.getDeclaredFields())
            {
                if (name.equals(field.getName()) && (type == null || type.equals(field.getType())))
                {
                    return field;
                }
            }

            search = search.getSuperclass();
        }

        throw new RuntimeException("No field with name '" + name + "' found in class hierarchy of '" + targetClass.getName() + "'");
    }
}