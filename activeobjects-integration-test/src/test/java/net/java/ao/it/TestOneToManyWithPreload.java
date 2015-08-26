package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.OneToMany;
import net.java.ao.Preload;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

@Data(TestOneToManyWithPreload.OneToManyWithPreloadDatabaseUpdater.class)
public final class TestOneToManyWithPreload extends ActiveObjectsIntegrationTest {
    @Test
    public void getManyElementsWithPreload() throws Exception {
        final Company c = newCompany("My Company");

        newPerson(c, "M. Brown");
        newPerson(c, "M. Black");
        newPerson(c, "M. White");

        c.getPersons();
    }

    private Person newPerson(Company c, String name) throws SQLException {
        final Person p = entityManager.create(Person.class);
        p.setName(name);
        p.setCompany(c);
        p.save();
        return p;
    }

    private Company newCompany(String name) throws SQLException {
        final Company c = entityManager.create(Company.class);
        c.setName(name);
        c.save();
        return c;
    }

    @Preload
    static interface Company extends Entity {
        void setName(String name);

        String getName();

        @OneToMany
        Person[] getPersons();
    }

    @Preload
    static interface Person extends Entity {
        void setName(String name);

        String getName();

        Company getCompany();

        void setCompany(Company c);
    }

    public static final class OneToManyWithPreloadDatabaseUpdater implements DatabaseUpdater {
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(Person.class, Company.class);
        }
    }
}
