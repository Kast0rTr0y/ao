package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Query;
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.Profession;
import net.java.ao.it.model.Select;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Callable;

import static net.java.ao.it.DatabaseProcessor.CompanyData;
import static net.java.ao.it.DatabaseProcessor.PenData;
import static net.java.ao.it.DatabaseProcessor.PersonData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 *
 */
@Data(DatabaseProcessor.class)
@Jdbc(DynamicJdbcConfiguration.class)
public class EntityManagerIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testGetCheckID()
    {
        assertNull(entityManager.get(Person.class, PersonData.getId() + 1));
    }

    @Test
    public void testGetCache() throws Exception
    {
        entityManager.get(Person.class, PersonData.getId());

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call()
            {
                entityManager.get(Person.class, PersonData.getId());
                return null;
            }
        });
    }

    @Test
    public void testReservedGet()
    {
        assertNull(entityManager.get(Select.class, 123));
    }

    @Test
    public void testCreate1() throws Exception
    {
        final Company company = checkSqlExecuted(new Callable<Company>()
        {
            public Company call() throws Exception
            {
                return entityManager.create(Company.class);
            }
        });

        executeStatement("SELECT " + escapeKeyword("companyID") + " FROM " + getTableName(Company.class) + " WHERE " + escapeKeyword("companyID") + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (!res.next())
                        {
                            fail("Unable to find INSERTed company row");
                        }
                    }
                });
    }

    @Test
    public void testCreate2() throws Exception
    {
        final Company company = checkSqlExecuted(new Callable<Company>()
        {
            public Company call() throws Exception
            {
                return entityManager.create(Company.class, new DBParam("name", null));
            }
        });

        executeStatement("SELECT " + escapeKeyword("name") + " FROM " + getTableName(Company.class) + " WHERE " + escapeKeyword("companyID") + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals(null, res.getString("name"));
                        }
                        else
                        {
                            fail("Unable to find INSERTed company row");
                        }
                    }
                });
    }

    @Test
    public void testCreate3() throws Exception
    {
        final Person person = checkSqlExecuted(new Callable<Person>()
        {
            public Person call() throws Exception
            {
                return entityManager.create(Person.class, new DBParam("url", "http://www.codecommit.com"));
            }
        });

        executeStatement("SELECT " + escapeKeyword("url") + " FROM "
                + getTableName(Person.class) + " WHERE " + escapeKeyword("id") + " = ?",
                new StatementCallback()
                {

                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, person.getID());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("http://www.codecommit.com", res.getString("url"));
                        }
                        else
                        {
                            fail("Unable to find INSERTed person row");
                        }

                    }
                });
    }

    @Test
    public void testCreateWithMap1() throws Exception
    {
        final Company company = checkSqlExecuted(new Callable<Company>()
        {
            public Company call() throws Exception
            {
                return entityManager.create(Company.class, new HashMap<String, Object>()
                {{
                        put("name", null);
                    }});
            }
        });

        executeStatement("SELECT " + escapeKeyword("name") + " FROM " + getTableName(Company.class) + " WHERE " + escapeKeyword("companyID") + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setLong(1, company.getCompanyID());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals(null, res.getString("name"));
                        }
                        else
                        {
                            fail("Unable to find INSERTed company row");
                        }
                    }
                });
    }

    @Test
    public void testCreateWithMap2() throws Exception
    {
        final Person person = checkSqlExecuted(new Callable<Person>()
        {
            public Person call() throws Exception
            {
                return entityManager.create(Person.class, new HashMap<String, Object>()
                {{
                        put("url", "http://www.codecommit.com");
                    }});
            }
        });

        executeStatement("SELECT " + escapeKeyword("url") + " FROM " + getTableName(Person.class) + " WHERE " + escapeKeyword("id") + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, person.getID());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("http://www.codecommit.com", res.getString("url"));
                        }
                        else
                        {
                            fail("Unable to find INSERTed person row");
                        }

                    }
                });
    }

    @Test
    public void testDelete() throws Exception
    {
        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                entityManager.delete();
                return null;
            }
        });
    }

    @Test
    public void testFindCheckIDs() throws SQLException
    {
        final Company[] coolCompanies = entityManager.find(Company.class, escapeKeyword("cool") + " = ?", true);

        assertEquals(1, coolCompanies.length);
        assertEquals(CompanyData.getIds()[1], coolCompanies[0].getCompanyID());

        final Company[] allCompanies = entityManager.find(Company.class);

        assertEquals(CompanyData.getIds().length, allCompanies.length);

        for (Company c : allCompanies)
        {
            boolean found = false;
            for (long id : CompanyData.getIds())
            {
                if (c.getCompanyID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find key=" + c.getCompanyID());
            }
        }

        final Person[] people = entityManager.find(Person.class, escapeKeyword("profession") + " = ?", Profession.DEVELOPER);

        assertEquals(1, people.length);
        assertEquals(PersonData.getId(), people[0].getID());
    }

    @Test
    public void testFindCheckPreload() throws Exception
    {
        final Pen[] pens = entityManager.find(Pen.class);

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                for (Pen pen : pens)
                {
                    pen.getWidth();
                }
                return null;
            }
        });

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                for (Pen pen : pens)
                {
                    pen.getPerson();
                }
                return null;
            }
        });
    }

    @Test
    public void testFindCheckDefinedPrecache() throws Exception
    {
        final Person[] people = entityManager.find(Person.class, Query.select("id, firstName, lastName"));

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                for (Person person : people)
                {
                    person.getFirstName();
                    person.getLastName();
                }
                return null;
            }
        });

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                for (Person person : people)
                {
                    person.getURL();
                    person.getCompany();
                }
                return null;
            }
        });
    }

    @Test
    public void testFindWithSQL() throws SQLException
    {
        final String companyTableName = getTableName(Company.class);
        final String personTableName = getTableName(Person.class);

        Company[] coolCompanies = entityManager.findWithSQL(Company.class,
                "companyID", "SELECT " + escapeKeyword("companyID") + " FROM "
                        + companyTableName + " WHERE " + escapeKeyword("cool") + " = ?", true);

        assertEquals(1, coolCompanies.length);
        assertEquals(CompanyData.getIds()[1], coolCompanies[0].getCompanyID());

        final Company[] allCompanies = entityManager.findWithSQL(Company.class, "companyID", "SELECT " + escapeKeyword("companyID") + " FROM " + companyTableName);

        assertEquals(CompanyData.getIds().length, allCompanies.length);

        for (Company c : allCompanies)
        {
            boolean found = false;
            for (long id : CompanyData.getIds())
            {
                if (c.getCompanyID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find key=" + c.getCompanyID());
            }
        }

        final Company company = entityManager.get(Company.class, CompanyData.getIds()[0]);
        Person[] people = entityManager.findWithSQL(Person.class, "id", "SELECT " + escapeKeyword("id") + " FROM "
                + personTableName
                + " WHERE " + escapeKeyword("companyID") + " = ?", company);
        Person[] companyPeople = company.getPeople();

        assertEquals(companyPeople.length, people.length);

        for (Person p : people)
        {
            boolean found = false;
            for (Person expectedPerson : companyPeople)
            {
                if (p.equals(expectedPerson))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                fail("Unable to find key=" + p.getID());
            }
        }
    }

    @Test
    public void testCount() throws SQLException
    {
        assertEquals(1, entityManager.count(Company.class, escapeKeyword("cool") + " = ?", true));
        assertEquals(PenData.getIds().length, entityManager.count(Pen.class));
        assertEquals(1, entityManager.count(Person.class));
        assertEquals(0, entityManager.count(Select.class));
    }
}
