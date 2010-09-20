package net.java.ao.schema;

import com.google.common.collect.ImmutableList;
import net.java.ao.EntityManager;
import net.java.ao.it.model.backup.Animal;
import net.java.ao.test.jdbc.DatabaseUpdater;

import java.util.List;

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
        entityManager.migrate(Animal.class);

        addAnimals(entityManager);
    }

    private void addAnimals(EntityManager entityManager) throws Exception
    {
        for (AnimalData animalData : AnimalData.ALL)
        {
            addAnimal(entityManager, animalData);
        }
    }

    private void addAnimal(EntityManager entityManager, AnimalData animalData) throws Exception
    {
        final Animal animal = entityManager.create(Animal.class);
        animal.setName(animalData.name);
        animal.save();
    }

    static final class AnimalData
    {
        public final String name;

        public static final List<AnimalData> ALL = ImmutableList.<AnimalData>builder()
                .add(new AnimalData("Horse"))
                .add(new AnimalData("Pelican"))
                .add(new AnimalData("Whale"))
                .build();

        private AnimalData(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "Animal : " + name;
        }
    }
}
