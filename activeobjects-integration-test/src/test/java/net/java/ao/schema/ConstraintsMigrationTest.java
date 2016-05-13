package net.java.ao.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import net.java.ao.Entity;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SchemaReader;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.SQLException;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for NotNull and Default constraints
 */
public final class ConstraintsMigrationTest extends ActiveObjectsIntegrationTest {
    /**
     * Remove not null constraint
     */
    @Test
    @NonTransactional
    public void testRemoveNotNullConstraint() throws Exception {
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
    @NonTransactional
    public void testAddNotNullConstraint() throws Exception {
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
    @NonTransactional
    public void testAddDefaultConstraint() throws Exception {
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
    @NonTransactional
    public void testRemoveDefaultConstraint() throws Exception {
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
    @NonTransactional
    public void testNotNullAndDefaultToNotNullConstraint() throws Exception {
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
    @NonTransactional
    public void testNotNullToNotNullAndDefaultConstraint() throws Exception {
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
    @NonTransactional
    public void testNoneToNotNullAndDefaultConstraint() throws Exception {
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
    @NonTransactional
    public void testNotNullAndDefaultToNoConstraint() throws Exception {
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
    @NonTransactional
    public void testAddIndex() throws Exception {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoIndexedColumn.T.class);
        assertIndex(false);

        entityManager.migrate(IndexedColumn.T.class);
        assertIndex(true);
    }

    @Test
    @NonTransactional
    public void testRemoveIndex() throws Exception {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(IndexedColumn.T.class);
        assertIndex(true);

        entityManager.migrate(NoIndexedColumn.T.class);
        assertIndex(false);
    }

    @Test
    @NonTransactional
    public void testShouldAddCompositeIndex() throws Exception {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(NoCompositeIndex.T.class);
        assertIndex(false);

        entityManager.migrate(CompositeIndex.T.class);
        assertIndex(true);
    }

    @Test
    @NonTransactional
    public void testShouldRemoveCompositeIndex() throws Exception {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(CompositeIndex.T.class);
        assertIndex(true);

        entityManager.migrate(NoCompositeIndex.T.class);
        assertIndex(false);
    }

    /**
     * Add a unique constraint to a column
     */
    @Test
    @NonTransactional
    public void testAddUniqueConstraint() throws Exception {
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
    @NonTransactional
    public void testRemoveUniqueConstraint() throws Exception {
        // first create with constraint
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(UniqueConstraintColumn.T.class);
        assertUniqueConstraint(true);

        entityManager.migrate(NoUniqueConstraintColumn.T.class);
        assertUniqueConstraint(false);
    }

    @Test
    @NonTransactional
    public void testUpdateConstraintWithForeignKey() throws Exception {
        entityManager.migrate(Clean.T.class);
        assertEmpty();

        entityManager.migrate(WithForeignKey.T.class);
        assertNullConstraint(false);
        assertHasForeignKey(true);
        assertIndex(true);

        entityManager.migrate(WithForeignKeyAndNotNull.T.class);
        assertNullConstraint(true);
        assertHasForeignKey(true);
        assertIndex(true);
    }

    private void assertEmpty() throws Exception {
        assertEquals(1, getDdlTable().getFields().length);
    }

    private void assertNullConstraint(boolean set) throws Exception {
        for (DDLField field : getDdlTable().getFields()) {
            if (field.getName().equalsIgnoreCase("name")) {
                assertEquals("Null constraint should " + (set ? "" : "not ") + "be set.", set, field.isNotNull());
            }
        }
    }

    private void assertDefaultConstraint(boolean set) throws Exception {
        for (DDLField field : getDdlTable().getFields()) {
            if (field.getName().equalsIgnoreCase("name")) {
                assertTrue("Default constraint should " + (set ? "" : "not ") + "be set.", set == (field.getDefaultValue() != null));
            }
        }
    }

    private void assertIndex(boolean set) throws Exception {
        assertEquals(set, getDdlTable().getIndexes().length > 0);
    }

    private void assertUniqueConstraint(boolean set) throws Exception {
        for (DDLField field : getDdlTable().getFields()) {
            if (field.getName().equalsIgnoreCase("name")) {
                assertTrue("Unique constraint should " + (set ? "" : "not ") + "be set.", set == field.isUnique());
            }
        }
    }

    private void assertHasForeignKey(boolean set) throws Exception {
        final DDLTable table = getDdlTable();
        assertEquals("Foreign key constraint should " + (set ? "" : "NOT ") + "exist.", set, Iterables.any(newArrayList(table.getForeignKeys()), new Predicate<DDLForeignKey>() {
            @Override
            public boolean apply(DDLForeignKey fk) {
                return fk.getField().equalsIgnoreCase("name_id");
            }
        }));
    }

    private DDLTable getDdlTable() throws SQLException {
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) getFieldValue(entityManager, "schemaConfiguration");
        final DDLTable[] tables = SchemaReader.readSchema(entityManager.getProvider(), entityManager.getNameConverters(), schemaConfiguration);
        return findTable(tables, entityManager.getNameConverters().getTableNameConverter().getName(Clean.T.class));
    }

    private DDLTable findTable(DDLTable[] tables, final String name) {
        return Iterables.find(newArrayList(tables), new Predicate<DDLTable>() {
            @Override
            public boolean apply(DDLTable t) {
                return t.getName().equalsIgnoreCase(name);
            }
        });
    }

    // Reflection tools
    public static Object getFieldValue(Object target, String name) {
        try {
            Field field = findField(name, target.getClass());
            field.setAccessible(true);
            return field.get(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Field findField(String name, Class<?> targetClass) {
        return findField(name, targetClass, null);
    }

    public static Field findField(String name, Class<?> targetClass, Class<?> type) {
        Class<?> search = targetClass;
        while (!Object.class.equals(search) && search != null) {
            for (Field field : search.getDeclaredFields()) {
                if (name.equals(field.getName()) && (type == null || type.equals(field.getType()))) {
                    return field;
                }
            }

            search = search.getSuperclass();
        }

        throw new RuntimeException("No field with name '" + name + "' found in class hierarchy of '" + targetClass.getName() + "'");
    }

    static class Clean {
        /**
         * Not Null Constraint Column
         */
        public static interface T extends Entity {
        }
    }

    static class NotNullConstraint {
        /**
         * Not Null Constraint Column
         */
        public static interface T extends Entity {
            @NotNull
            public String getName();

            public void setName(String name);
        }
    }

    static class WithoutNotNullConstraint {
        /**
         * Without null constraint
         */
        public static interface T extends Entity {
            public String getName();

            public void setName(String name);
        }
    }

    static class DefaultConstraint {
        /**
         * With default constraint
         */
        public static interface T extends Entity {
            @Default("Test")
            public String getName();

            public void setName(String name);
        }
    }

    static class EmptyDefaultConstraint {
        /**
         * With default constraint
         */
        public static interface T extends Entity {
            @Default("")
            public String getName();

            public void setName(String name);
        }
    }

    static class NoDefaultConstraint {
        /**
         * With default constraint
         */
        public static interface T extends Entity {
            public String getName();

            public void setName(String name);
        }
    }

    static class NullAndDefaultConstraint {
        /**
         * Not null and default constraint
         */
        public static interface T extends Entity {
            @NotNull
            @Default("Test")
            public String getName();

            public void setName(String name);
        }
    }

    static class NullAndNoDefaultConstraint {
        /**
         * Not null without default constraint
         */
        public static interface T extends Entity {
            @NotNull
            public String getName();

            public void setName(String name);
        }
    }

    static class IndexedColumn {
        /**
         * With default constraint
         */
        public static interface T extends Entity {
            @Indexed
            public String getName();

            public void setName(String name);
        }
    }

    static class NoIndexedColumn {
        /**
         * With default constraint
         */
        public static interface T extends Entity {
            public String getName();

            public void setName(String name);
        }
    }

    static class CompositeIndex {
        @Indexes(
                @Index(name = "indx", methodNames = {"getName", "getAge"})
        )
        public interface T extends Entity {
            String getName();

            void setName(String name);

            String getAge();

            void setAge(String age);
        }
    }

    static class NoCompositeIndex {
        public interface T extends Entity {
            String getName();

            void setName(String name);

            String getAge();

            void setAge(String age);
        }
    }

    static class UniqueConstraintColumn {
        /**
         * With unique constraint constraint
         */
        public static interface T extends Entity {
            @Unique
            public String getName();

            public void setName(String name);
        }
    }

    static class NoUniqueConstraintColumn {
        public static interface T extends Entity {
            public String getName();

            public void setName(String name);
        }
    }

    static class WithForeignKey {
        public static interface T extends Entity {
            public U getName();

            public void setName(U u);
        }

        public static interface U extends Entity {
        }
    }

    static class WithForeignKeyAndNotNull {
        public static interface T extends Entity {
            @NotNull
            public U getName();

            @NotNull
            public void setName(U u);
        }

        public static interface U extends Entity {
        }
    }
}
