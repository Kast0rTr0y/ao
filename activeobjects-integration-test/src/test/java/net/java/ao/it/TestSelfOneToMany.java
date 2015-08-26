package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.EntityManager;
import net.java.ao.OneToMany;
import net.java.ao.Preload;
import net.java.ao.Query;
import net.java.ao.RawEntity;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Data(TestSelfOneToMany.OneToManyDatabaseUpdater.class)
public final class TestSelfOneToMany extends ActiveObjectsIntegrationTest {
    @Test
    public void testEmptyRelationElements() throws Exception {
        newCompany("id1", "My company", null);
        Company[] c = entityManager.find(Company.class);
        assertEquals("Result.size", 1, c.length);
        assertEquals("Result.name", "My company", c[0].getName());
        assertNull("Result.subcompany", c[0].getSubCompany());
    }

    @Test
    public void testExistingRelationElements() throws Exception {
        Company child = newCompany("id2", "Child", null);
        ;
        newCompany("id1", "Parent", child);

        Company[] c = entityManager.find(Company.class, Query.select().order("ID ASC"));
        assertEquals("Result.size", 2, c.length);
        assertEquals("Result[0].name", "Parent", c[0].getName());
        assertEquals("Result[1].name", "Child", c[1].getName());
        assertEquals("Result[0].subcompany", child, c[0].getSubCompany());
        assertNull("Result[1].subcompany", c[1].getSubCompany());
    }

    private Company newCompany(String id, String name, Company child) throws SQLException {
        final Company c = entityManager.create(Company.class, new DBParam("ID", id));
        c.setSubCompany(child);
        c.setName(name);
        c.save();
        return c;
    }

    @Preload
    static interface Company extends RawEntity<String> {
        @PrimaryKey
        @NotNull
        String getID();

        void setName(String name);

        String getName();

        Company getParent();

        void setParent(Company parentEntity);

        @OneToMany(reverse = "getParent")
        Company[] getChildren();


        Company getSubCompany();

        void setSubCompany(Company c);

        @OneToMany(reverse = "getSubCompany")
        Company[] getSubCompanies();
    }

    public static final class OneToManyDatabaseUpdater implements DatabaseUpdater {
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(Company.class);
        }
    }
}
