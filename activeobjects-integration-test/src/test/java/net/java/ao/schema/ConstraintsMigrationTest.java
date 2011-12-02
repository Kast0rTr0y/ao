package net.java.ao.schema;

import net.java.ao.Entity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.*;

import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests for NotNull and Default constraints
 */
public final class ConstraintsMigrationTest extends ActiveObjectsIntegrationTest
{
    /**
     * Remove not null constraint
     */
    @Test
    public void testRemoveNotNullConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NotNullConstraint.T.class);
        assertNullConstraint(true);

        entityManager.migrate(WithoutNotNullConstraint.T.class);
        assertNullConstraint(false);
    }

    /**
     * Add not null constraint
     */
    @Test
    public void testAddNotNullConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(WithoutNotNullConstraint.T.class);
        assertNullConstraint(false);

        entityManager.migrate(NotNullConstraint.T.class);
        assertNullConstraint(true);
    }

    /**
     * Add a default constraint
     */
    @Test
    public void testAddDefaultConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoDefaultConstraint.T.class);
        assertDefaultConstraint(false);

        entityManager.migrate(DefaultConstraint.T.class);
        assertDefaultConstraint(true);
    }

    /**
     * Remove a default constraint
     */
    @Test
    public void testRemoveDefaultConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(DefaultConstraint.T.class);
        assertDefaultConstraint(true);

        entityManager.migrate(NoDefaultConstraint.T.class);
        assertDefaultConstraint(false);
    }

    /**
     * From not null and default constraint to not null constraint
     */
    @Test
    public void testNotNullAndDefaultToNotNullConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NullAndDefaultConstraint.T.class);
        assertDefaultConstraint(true);
        assertNullConstraint(true);

        entityManager.migrate(NullAndNoDefaultConstraint.T.class);
        assertDefaultConstraint(false);
        assertNullConstraint(true);
    }

    /**
     * From not null constraint to not null and default constraint
     */
    @Test
    public void testNotNullToNotNullAndDefaultConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NullAndNoDefaultConstraint.T.class);
        assertDefaultConstraint(false);
        assertNullConstraint(true);

        entityManager.migrate(NullAndDefaultConstraint.T.class);
        assertDefaultConstraint(true);
        assertNullConstraint(true);
    }

    /**
     * From no constraint to not null and default constraint
     */
    @Test
    public void testNoneToNotNullAndDefaultConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoDefaultConstraint.T.class);
        assertDefaultConstraint(false);
        assertNullConstraint(false);

        entityManager.migrate(NullAndDefaultConstraint.T.class);
        assertDefaultConstraint(true);
        assertNullConstraint(true);
    }

    /**
     * From not null and default constraint to no constraint
     */
    @Test
    public void testNotNullAndDefaultToNoConstraint() throws Exception
    {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NullAndDefaultConstraint.T.class);
        assertDefaultConstraint(true);
        assertNullConstraint(true);

        entityManager.migrate(NoDefaultConstraint.T.class);
        assertDefaultConstraint(false);
        assertNullConstraint(false);
    }

    /**
     * Add an index to a column
     * Note: Currently broken because schema migration does not check for indexes
     */
    @Test
    public void testAddIndex() throws Exception
    {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoIndexedColumn.T.class);
        assertIndex(false);

        entityManager.migrate(IndexedColumn.T.class);
        assertIndex(true);
    }

    /**
     * Remove an index from a column
     * Note: Currently broken because schema migration does not check for indexes
     */
    @Test
    public void testRemoveIndex() throws Exception
    {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(IndexedColumn.T.class);
        assertIndex(true);

        entityManager.migrate(NoIndexedColumn.T.class);
        assertIndex(false);
    }

    /**
     * Add a unique constraint to a column
     */
    @Test
    public void testAddUniqueConstraint() throws Exception
    {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoUniqueConstraintColumn.T.class);
        assertUniqueConstraint(false);

        entityManager.migrate(UniqueConstraintColumn.T.class);
        assertUniqueConstraint(true);
    }

    /**
     * Remove a unique constraint from a column
     */
    @Test
    public void testRemoveUniqueConstraint() throws Exception
    {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(UniqueConstraintColumn.T.class);
        assertUniqueConstraint(true);

        entityManager.migrate(NoUniqueConstraintColumn.T.class);
        assertUniqueConstraint(false);
    }

    private void assertEmpty() throws Exception
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
        assertEquals(1, tables[0].getFields().length);
    }

    private void assertNullConstraint(boolean set) throws Exception
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
        for (DDLField field : tables[0].getFields())
        {
            if (field.getName().equalsIgnoreCase("name"))
            {
                assertEquals("Null constraint should " + (set ? "" : "not ") + "be set.", set, field.isNotNull());
            }
        }
    }

    private void assertDefaultConstraint(boolean set) throws Exception
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
        for (DDLField field : tables[0].getFields())
        {
            if (field.getName().equalsIgnoreCase("name"))
            {
                assertTrue("Default constraint should " + (set ? "" : "not ") + "be set.", set == (field.getDefaultValue() != null));
            }
        }
    }

    private void assertIndex(boolean set) throws Exception
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);

        assertEquals(set, tables[0].getIndexes().length > 0);
    }

    private void assertUniqueConstraint(boolean set) throws Exception
    {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
        for (DDLField field : tables[0].getFields())
        {
            if (field.getName().equalsIgnoreCase("name"))
            {
                assertTrue("Unique constraint should " + (set ? "" : "not ") + "be set.", set == field.isUnique());
            }
        }
    }

    // Reflection tools
    public static Object getFieldValue(Object target, String name)
    {
        try
        {
            Field field = findField(name, target.getClass());
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

    static class Clean
    {
        /**
         * Not Null Constraint Column
         */
        @Table("clean")
        public static interface T extends Entity
        {
        }
    }

    static class NotNullConstraint
    {
        /**
         * Not Null Constraint Column
         */
        @Table("nulloholic")
        public static interface T extends Entity
        {
            @NotNull
            public String getName();

            public void setName(String name);
        }
    }

    static class WithoutNotNullConstraint
    {
        /**
         * Without null constraint
         */
        @Table("nulloholic")
        public static interface T extends Entity
        {
            public String getName();

            public void setName(String name);
        }
    }

    static class DefaultConstraint
    {
        /**
         * With default constraint
         */
        @Table("defaultoholic")
        public static interface T extends Entity
        {
            @Default("Test")
            public String getName();

            public void setName(String name);
        }
    }

    static class EmptyDefaultConstraint
    {
        /**
         * With default constraint
         */
        @Table("defaultoholic")
        public static interface T extends Entity
        {
            @Default("")
            public String getName();

            public void setName(String name);
        }
    }

    static class NoDefaultConstraint
    {
        /**
         * With default constraint
         */
        @Table("defaultoholic")
        public static interface T extends Entity
        {
            public String getName();

            public void setName(String name);
        }
    }

    static class NullAndDefaultConstraint
    {
        /**
         * Not null and default constraint
         */
        @Table("defaultoholic")
        public static interface T extends Entity
        {
            @NotNull
            @Default("Test")
            public String getName();

            public void setName(String name);
        }
    }

    static class NullAndNoDefaultConstraint
    {
        /**
         * Not null without default constraint
         */
        @Table("defaultoholic")
        public static interface T extends Entity
        {
            @NotNull
            public String getName();

            public void setName(String name);
        }
    }

    static class IndexedColumn
    {
        /**
         * With default constraint
         */
        @Table("indexoholic")
        public static interface T extends Entity
        {
            @Indexed
            public String getName();

            public void setName(String name);
        }
    }

    static class NoIndexedColumn
    {
        /**
         * With default constraint
         */
        @Table("indexoholic")
        public static interface T extends Entity
        {
            public String getName();

            public void setName(String name);
        }
    }

    static class UniqueConstraintColumn
    {
        /**
         * With unique constraint constraint
         */
        @Table("uniqueoholic")
        public static interface T extends Entity
        {
            @Unique
            public String getName();

            public void setName(String name);
        }
    }

    static class NoUniqueConstraintColumn
    {
        /**
         * With default constraint
         */
        @Table("uniqueoholic")
        public static interface T extends Entity
        {
            public String getName();

            public void setName(String name);
        }
    }
}
