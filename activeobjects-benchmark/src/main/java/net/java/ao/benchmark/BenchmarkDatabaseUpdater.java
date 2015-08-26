package net.java.ao.benchmark;

import net.java.ao.EntityManager;
import net.java.ao.benchmark.model.Person;
import net.java.ao.benchmark.model.PersonWithPreload;
import net.java.ao.test.jdbc.DatabaseUpdater;

public final class BenchmarkDatabaseUpdater implements DatabaseUpdater {
    @Override
    public void update(EntityManager entityManager) throws Exception {
        entityManager.migrate(Person.class, PersonWithPreload.class);
    }
}
