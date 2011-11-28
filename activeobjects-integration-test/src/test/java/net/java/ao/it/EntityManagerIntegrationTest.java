package net.java.ao.it;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import net.java.ao.DBParam;
import net.java.ao.EntityStreamCallback;
import net.java.ao.Query;
import net.java.ao.Query.QueryType;
import net.java.ao.it.DatabaseProcessor.CompanyData;
import net.java.ao.it.DatabaseProcessor.PenData;
import net.java.ao.it.DatabaseProcessor.PersonData;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.Profession;
import net.java.ao.it.model.Select;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.Data;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 *
 */
@Data(DatabaseProcessor.class)
public class EntityManagerIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testGetCheckID() throws Exception
    {
        assertNull(entityManager.get(Person.class, PersonData.getId() + 1));
    }

    @Test
    public void testGetCache() throws Exception
    {
        entityManager.get(Person.class, PersonData.getId());

        checkSqlNotExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                entityManager.get(Person.class, PersonData.getId());
                return null;
            }
        });
    }

    @Test
    public void testReservedGet() throws Exception
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

        executeStatement("SELECT " + escapeFieldName(Company.class, "getCompanyID") + " FROM " + getTableName(Company.class) + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
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
                return entityManager.create(Company.class, new DBParam(getFieldName(Company.class, "getName"), null));
            }
        });

        executeStatement("SELECT " + escapeFieldName(Company.class, "getName") + " FROM " + getTableName(Company.class) + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
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
                + getTableName(Person.class) + " WHERE " + escapeFieldName(Person.class, "getID") + " = ?",
                new DbUtils.StatementCallback()
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
                        put(getFieldName(Company.class, "getName"), null);
                    }});
            }
        });

        executeStatement("SELECT " + escapeFieldName(Company.class, "getName") + " FROM " + getTableName(Company.class) + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?",
                new DbUtils.StatementCallback()
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

        executeStatement("SELECT " + escapeKeyword("url") + " FROM " + getTableName(Person.class) + " WHERE " + escapeFieldName(Person.class, "getID") + " = ?",
                new DbUtils.StatementCallback()
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
        final Company[] coolCompanies = entityManager.find(Company.class, escapeFieldName(Company.class, "isCool") + " = ?", true);

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

        final Person[] people = entityManager.find(Person.class, escapeFieldName(Person.class, "getProfession") + " = ?", Profession.DEVELOPER);

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
        final Person[] people = entityManager.find(Person.class, Query.select(getFieldName(Person.class, "getID") + ", " + getFieldName(Person.class, "getFirstName") + ", " + getFieldName(Person.class, "getLastName")));

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
                getFieldName(Company.class, "getCompanyID"),
                "SELECT " + escapeFieldName(Company.class, "getCompanyID") + " FROM "
                        + companyTableName + " WHERE " + escapeFieldName(Company.class, "isCool") + " = ?", true);

        assertEquals(1, coolCompanies.length);
        assertEquals(CompanyData.getIds()[1], coolCompanies[0].getCompanyID());

        final Company[] allCompanies = entityManager.findWithSQL(Company.class, getFieldName(Company.class, "getCompanyID"),
                "SELECT " + escapeFieldName(Company.class, "getCompanyID") + " FROM " + companyTableName);

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
        Person[] people = entityManager.findWithSQL(Person.class, getFieldName(Person.class, "getID"),
                "SELECT " + escapeFieldName(Person.class, "getID") + " FROM "
                + personTableName
                + " WHERE " + escapeFieldName(Company.class, "getCompanyID") + " = ?", company);
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
    
    /**
     * Load all rows from the company table through the stream API
     */
    @Test
    public void testStream() throws SQLException {
        final List<Company> streamed = new ArrayList<Company>();
        
        // make sure we've got enough data
        assertTrue(CompanyData.NAMES.length > 1);
        
        entityManager.stream(Company.class, new EntityStreamCallback<Company, Long>()
        {
            @Override
            public void onRowRead(Company t)
            {
                streamed.add(t);
            }
        });
        
        assertEquals(CompanyData.ids.length, streamed.size());
        for (final String name : CompanyData.NAMES)
        {
            // throws exception if no element matches the predicate, which will fail test
            Iterables.find(streamed, new Predicate<Company>()
            {
                @Override
                public boolean apply(Company company)
                {
                    return name.equals(company.getName()); 
                }
            });
        }
    }
    
    /**
     * Load only one row from the company table through the stream API
     * @throws SQLException 
     */
    @Test
    public void testStreamWithQuery() throws SQLException {
        final List<Company> streamed = new ArrayList<Company>();
        
        // make sure we've got enough data
        assertTrue(CompanyData.NAMES.length > 1);
        
        Query query = new Query(QueryType.SELECT, "*").from(Company.class).where(getFieldName(Company.class, "getName") + " = ?", CompanyData.NAMES[0]);
        entityManager.stream(Company.class, query, new EntityStreamCallback<Company, Long>()
        {
            @Override
            public void onRowRead(Company t)
            {
                streamed.add(t);
            }
        });
        
        assertEquals("There should have only been one row matching the query", 1, streamed.size());
        assertEquals(CompanyData.NAMES[0], streamed.get(0).getName());
    }
    
    /**
     * Query matches no rows
     */
    @Test
    public void testStreamWithQueryNoResult() throws SQLException {
        final List<Company> streamed = new ArrayList<Company>();
        
        // make sure we've got enough data
        assertTrue(CompanyData.NAMES.length > 1);
        
        Query query = new Query(QueryType.SELECT, "*").from(Company.class).where(getFieldName(Company.class, "getName") + " = ?", "NotInTheDatabase");
        entityManager.stream(Company.class, query, new EntityStreamCallback<Company, Long>()
        {
            @Override
            public void onRowRead(Company t)
            {
                streamed.add(t);
            }
        });
        
        assertTrue("No rows should have matched the query", streamed.isEmpty());
    }
    
    /**
     * Query with limited fields, should be expanded to full query for stream
     */
    @Test
    public void testStreamWithQueryLimitedFields() throws SQLException {
        final List<Company> streamed = new ArrayList<Company>();
        
        // make sure we've got enough data
        assertTrue(CompanyData.NAMES.length > 1);
        
        Query query = new Query(QueryType.SELECT, getFieldName(Company.class, "getCompanyID")).from(Company.class).where(getFieldName(Company.class, "getName") + " = ?", CompanyData.NAMES[0]);
        entityManager.stream(Company.class, query, new EntityStreamCallback<Company, Long>()
        {
            @Override
            public void onRowRead(Company t)
            {
                streamed.add(t);
            }
        });
        
        assertEquals("There should have only been one row matching the query", 1, streamed.size());
        assertNull("name field should not have been preloaded", streamed.get(0).getName());        
        assertNotNull("ID field should have been preloaded", streamed.get(0).getCompanyID());
    }

    @Test
    public void testCount() throws SQLException
    {
        assertEquals(1, entityManager.count(Company.class, escapeFieldName(Company.class, "isCool") + " = ?", true));
        assertEquals(PenData.getIds().length, entityManager.count(Pen.class));
        assertEquals(1, entityManager.count(Person.class));
        assertEquals(0, entityManager.count(Select.class));
    }
}
