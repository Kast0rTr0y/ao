package net.java.ao.schema;

import com.google.common.collect.ImmutableList;
import net.java.ao.EntityManager;
import net.java.ao.it.model.backup.Animal;
import net.java.ao.it.model.backup.AnimalClass;
import net.java.ao.test.jdbc.DatabaseUpdater;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.Lists.newLinkedList;

/**
 *
 */
public class BackupRestoreDatabaseUpdater implements DatabaseUpdater
{
    public void update(EntityManager entityManager) throws Exception
    {
        // dropping all the tables
        entityManager.migrate();

        // creating the schema
        entityManager.migrate(Animal.class, AnimalClass.class);


        final AnimalClass mammals = addClass(entityManager, "Mammals");
        addAnimal(entityManager, "Horse", mammals);
        addAnimal(entityManager, "Whale", mammals);

        final AnimalClass birds = addClass(entityManager, "Birds");
        addAnimal(entityManager, "Pelican", birds);
    }

    private AnimalClass addClass(EntityManager entityManager, String className) throws Exception
    {
        final AnimalClass animalClass = entityManager.create(AnimalClass.class);
        animalClass.setName(className);
        animalClass.save();

        AnimalClassData.add(className);

        return animalClass;
    }

    private Animal addAnimal(EntityManager entityManager, String name, AnimalClass animalClass) throws Exception
    {
        final Animal animal = entityManager.create(Animal.class);
        animal.setName(name);
        animal.setAnimalClass(animalClass);
        animal.save();

        AnimalData.add(name, animalClass.getID());

        return animal;
    }

    static final class AnimalClassData
    {
        private static final Collection<AnimalClassData> ALL = newLinkedList();
        public final String name;

        public AnimalClassData(String name)
        {
            this.name = name;
        }

        public static List<AnimalClassData> all()
        {
            return ImmutableList.copyOf(ALL);
        }

        static void add(String name)
        {
            ALL.add(new AnimalClassData(name));
        }
    }

    static final class AnimalData
    {
        private static final Collection<AnimalData> ALL = newLinkedList();

        public final String name;
        public final int classId;

        private AnimalData(String name, int classId)
        {
            this.name = name;
            this.classId = classId;
        }

        @Override
        public String toString()
        {
            return "Animal : " + name;
        }

        public static List<AnimalData> all()
        {
            return ImmutableList.copyOf(ALL);
        }

        static void add(String name, int classId)
        {
            ALL.add(new AnimalData(name, classId));
        }
    }
}
