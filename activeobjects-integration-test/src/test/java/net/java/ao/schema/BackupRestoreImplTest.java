package net.java.ao.schema;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.java.ao.RawEntity;
import net.java.ao.it.model.backup.Animal;
import net.java.ao.it.model.backup.AnimalClass;
import net.java.ao.schema.ddl.DDLAction;
import net.java.ao.schema.ddl.DDLActionType;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Hsql;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.*;
import static net.java.ao.schema.BackupRestoreDatabaseUpdater.*;
import static org.junit.Assert.*;

/**
 *
 */
@Data(BackupRestoreDatabaseUpdater.class)
@Jdbc(Hsql.class)
@RunWith(ActiveObjectsJUnitRunner.class)
public class BackupRestoreImplTest extends AbstractBackupRestoreTest
{
    @Test
    @NonTransactional
    public void testBackupAndRestore() throws Exception
    {
        final List<DDLAction> originalBackup = backup();

        assertBackupIsCorrect(originalBackup);

        emptyDatabase();

        restore(originalBackup);

        assertDatabaseIsNotEmpty();

        final Collection<Animal> animals = newArrayList(entityManager.find(Animal.class));
        for (final AnimalData animalData : AnimalData.all())
        {
            assertAnimalExists(animals, animalData);
        }

        final Collection<AnimalClass> animalClasses = newArrayList(entityManager.find(AnimalClass.class));
        for (AnimalClassData animalClassData : AnimalClassData.all())
        {
            assertAnimalClassesExists(animalClasses, animalClassData);
        }

        final List<DDLAction> backupAfterRestore = backup();
        assertBackupIsCorrect(backupAfterRestore); // second backup from restored DB should be the same as original backup
    }

    private void assertBackupIsCorrect(Iterable<DDLAction> backup)
    {
        final Iterator<DDLAction> backupIt = backup.iterator();

        assertIsDropForeignKey(backupIt);
        assertIsDropTables(backupIt, Animal.class, AnimalClass.class);
        assertCreateTables(backupIt, Animal.class, AnimalClass.class);
        assertIsInsert(Animal.class, 3, backupIt);
        assertIsInsert(AnimalClass.class, 2, backupIt);

        assertIsCreateForeignKey(backupIt.next());
    }

    private void assertIsDropForeignKey(Iterator<DDLAction> ddlActions)
    {
        assertIsActionType(DDLActionType.ALTER_DROP_KEY, ddlActions.next());
    }

    private void assertIsCreateForeignKey(DDLAction ddlAction)
    {
        assertIsActionType(DDLActionType.ALTER_ADD_KEY, ddlAction);
    }

    private void assertIsActionType(DDLActionType expectedType, DDLAction ddlAction)
    {
        assertEquals(expectedType, ddlAction.getActionType());
    }

    private void assertCreateTables(Iterator<DDLAction> backupIt, Class<? extends RawEntity<?>>... entityClass)
    {
        final List<String> tableNames = Lists.transform(newArrayList(entityClass), new Function<Class<? extends RawEntity<?>>, String>()
        {
            public String apply(Class<? extends RawEntity<?>> from)
            {
                return getTableNameIgnoreCase(getTableNameConverter().getName(from));
            }
        });

        while (!tableNames.isEmpty())
        {
            final DDLAction action = backupIt.next();
            assertIsActionType(DDLActionType.CREATE, action);
            assertTrue(tableNames.remove(getTableNameIgnoreCase(action.getTable().getName())));
        }
    }

    private static void assertAnimalClassesExists(Collection<AnimalClass> animalClasses, final AnimalClassData animalClassData)
    {
        assertExists(animalClasses, new Predicate<AnimalClass>()
        {
            public boolean apply(AnimalClass animalClass)
            {
                return animalClass != null
                        && animalClass.getName().equals(animalClassData.name);
            }
        });
    }

    private static void assertAnimalExists(Collection<Animal> animals, final AnimalData animalData)
    {
        assertExists(animals, new Predicate<Animal>()
        {
            public boolean apply(Animal animal)
            {
                return animal != null
                        && animal.getName().equals(animalData.name)
                        && animal.getAnimalClass().getID() == animalData.classId;
            }
        });
    }

    private static <E> void assertExists(Collection<E> entities, Predicate<E> entityPredicate)
    {
        assertEquals("Should have found one and only one", 1, size(filter(entities, entityPredicate)));
    }

    private void assertIsDropTables(Iterator<DDLAction> ddlActions, Class<? extends RawEntity<?>>... entityClass)
    {
        final Collection<Class<? extends RawEntity<?>>> entityClasses = newArrayList(entityClass);
        while (!entityClasses.isEmpty())
        {
            final DDLAction ddlAction = ddlActions.next();
            assertIsActionType(DDLActionType.DROP, ddlAction);
            final Class<? extends RawEntity<?>> found = Iterables.find(entityClasses, new Predicate<Class<? extends RawEntity<?>>>()
            {
                public boolean apply(Class<? extends RawEntity<?>> input)
                {
                    return tableNamesEquals(input, ddlAction.getTable());
                }
            });
            assertTrue(entityClasses.remove(found));
        }
    }

    private void assertIsInsert(Class<? extends RawEntity<?>> entityClass, int rows, Iterator<DDLAction> ddlActions)
    {
        for (int i = 0; i < rows; i++)
        {
            assertIsInsert(entityClass, ddlActions.next());
        }
    }

    private void assertIsInsert(Class<? extends RawEntity<?>> entityClass, DDLAction ddlAction)
    {
        assertIsActionType(DDLActionType.INSERT, ddlAction);
        assertTableNameEquals(entityClass, ddlAction.getTable());
    }

    private void assertTableNameEquals(Class<? extends RawEntity<?>> entityClass, DDLTable ddlTable)
    {
        assertTrue(tableNamesEquals(entityClass, ddlTable));
    }

    private boolean tableNamesEquals(Class<? extends RawEntity<?>> entityClass, DDLTable ddlTable)
    {
        return getTableNameIgnoreCase(getTableName(entityClass)).equals(getTableNameIgnoreCase(ddlTable.getName()));
    }
}
