/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import net.java.ao.builder.EntityManagerBuilder;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import org.junit.Test;
import test.schema.Company;
import test.schema.Pen;
import test.schema.Person;
import test.schema.Profession;
import test.schema.Select;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static net.java.ao.TestUtilities.postgresName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author Daniel Spiewak
 */
public class EntityManagerTest extends DataTest
{
    @Test
    public void testGetCheckID()
    {
        assertNull(manager.get(Person.class, personID + 1));
    }

    @Test
    public void testGetCache() throws Exception
    {
        manager.get(Person.class, personID);

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run()
            {
                manager.get(Person.class, personID);
                return null;
            }
        });
    }

    @Test
    public void testReservedGet()
    {
        assertNull(manager.get(Select.class, 123));
    }

    @Test
    public void testCreate() throws Exception
    {
        Company company = sql.checkExecuted(new Command<Company>()
        {
            public Company run() throws Exception
            {
                return manager.create(Company.class);
            }
        });

        String companyTableName = manager.getTableNameConverter().getName(Company.class);
        companyTableName = manager.getProvider().processID(companyTableName);

        String personTableName = manager.getTableNameConverter().getName(Person.class);
        personTableName = manager.getProvider().processID(personTableName);

        Connection conn = manager.getProvider().getConnection();
        try
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT " + postgresName("companyID")
                    + " FROM " + postgresName(companyTableName)
                    + " WHERE " + postgresName("companyID") + " = ?");
            stmt.setLong(1, company.getCompanyID());

            ResultSet res = stmt.executeQuery();
            if (!res.next())
            {
                fail("Unable to find INSERTed company row");
            }
            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
        }

        manager.delete(company);

        company = manager.create(Company.class, new DBParam("name", null));

        conn = manager.getProvider().getConnection();
        try
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT " + postgresName("name") + " FROM " + postgresName(companyTableName)
                    + " WHERE " + postgresName("companyID") + " = ?");
            stmt.setLong(1, company.getCompanyID());

            ResultSet res = stmt.executeQuery();

            if (res.next())
            {
                assertEquals(null, res.getString("name"));
            }
            else
            {
                fail("Unable to find INSERTed company row");
            }

            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
        }

        manager.delete(company);

        Person person = sql.checkExecuted(new Command<Person>()
        {
            public Person run() throws Exception
            {
                return manager.create(Person.class, new DBParam("url", "http://www.codecommit.com"));
            }
        });

        conn = manager.getProvider().getConnection();
        try
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT " + postgresName("url") + " FROM "
                    + postgresName(personTableName) + " WHERE " + postgresName("id") + " = ?");
            stmt.setInt(1, person.getID());

            ResultSet res = stmt.executeQuery();

            if (res.next())
            {
                assertEquals("http://www.codecommit.com", res.getString("url"));
            }
            else
            {
                fail("Unable to find INSERTed person row");
            }

            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
        }

        manager.delete(person);
    }

    @Test
    public void testCreateWithMap() throws Exception
    {
        String companyTableName = manager.getTableNameConverter().getName(Company.class);
        companyTableName = manager.getProvider().processID(companyTableName);

        String personTableName = manager.getTableNameConverter().getName(Person.class);
        personTableName = manager.getProvider().processID(personTableName);

        Company company = sql.checkExecuted(new Command<Company>()
        {
            public Company run() throws Exception
            {
                return manager.create(Company.class, new HashMap<String, Object>()
                {{
                        put("name", null);
                    }});
            }
        });

        Connection conn = manager.getProvider().getConnection();
        try
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT " + postgresName("name") + " FROM "
                    + postgresName(companyTableName)
                    + " WHERE " + postgresName("companyID") + " = ?");
            stmt.setLong(1, company.getCompanyID());

            ResultSet res = stmt.executeQuery();

            if (res.next())
            {
                assertEquals(null, res.getString("name"));
            }
            else
            {
                fail("Unable to find INSERTed company row");
            }

            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
        }

        manager.delete(company);

        final Person person = sql.checkExecuted(new Command<Person>()
        {
            public Person run() throws Exception
            {
                return manager.create(Person.class, new HashMap<String, Object>()
                {{
                        put("url", "http://www.codecommit.com");
                    }});
            }
        });

        conn = manager.getProvider().getConnection();
        try
        {
            PreparedStatement stmt = conn.prepareStatement("SELECT " + postgresName("url") + " FROM "
                    + postgresName(personTableName) + " WHERE " + postgresName("id") + " = ?");
            stmt.setInt(1, person.getID());

            ResultSet res = stmt.executeQuery();

            if (res.next())
            {
                assertEquals("http://www.codecommit.com", res.getString("url"));
            }
            else
            {
                fail("Unable to find INSERTed person row");
            }

            res.close();
            stmt.close();
        }
        finally
        {
            conn.close();
        }

        manager.delete(person);
    }

    @Test
    public void testDelete() throws Exception
    {
        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                manager.delete();
                return null;
            }
        });
    }

    @Test
    public void testFindCheckIDs() throws SQLException
    {
        Company[] coolCompanies = manager.find(Company.class, postgresName("cool") + " = ?", true);

        assertEquals(coolCompanyIDs.length, coolCompanies.length);

        for (Company c : coolCompanies)
        {
            boolean found = false;
            for (long id : coolCompanyIDs)
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

        Company[] companies = manager.find(Company.class);

        assertEquals(coolCompanyIDs.length + 1, companies.length);

        for (Company c : companies)
        {
            boolean found = false;
            for (long id : coolCompanyIDs)
            {
                if (c.getCompanyID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (c.getCompanyID() == companyID)
            {
                found = true;
            }

            if (!found)
            {
                fail("Unable to find key=" + c.getCompanyID());
            }
        }

        Person[] people = manager.find(Person.class, postgresName("profession") + " = ?", Profession.DEVELOPER);

        assertEquals(1, people.length);
        assertEquals(personID, people[0].getID());
    }

    @Test
    public void testFindCheckPreload() throws Exception
    {
        final Pen[] pens = manager.find(Pen.class);

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                for (Pen pen : pens)
                {
                    pen.getWidth();
                }
                return null;
            }
        });

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
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
        final Person[] people = manager.find(Person.class, Query.select(postgresName("id") + ", "
                + postgresName("firstName") + ", " + postgresName("lastName")));

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                for (Person person : people)
                {
                    person.getFirstName();
                    person.getLastName();
                }
                return null;
            }
        });

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
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
        String companyTableName = manager.getTableNameConverter().getName(Company.class);
        companyTableName = manager.getProvider().processID(companyTableName);

        String personTableName = manager.getTableNameConverter().getName(Person.class);
        personTableName = manager.getProvider().processID(personTableName);

        Company[] coolCompanies = manager.findWithSQL(Company.class,
                "companyID", "SELECT " + postgresName("companyID") + " FROM "
                        + postgresName(companyTableName) + " WHERE " + postgresName("cool") + " = ?", true);

        assertEquals(coolCompanyIDs.length, coolCompanies.length);

        for (Company c : coolCompanies)
        {
            boolean found = false;
            for (long id : coolCompanyIDs)
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

        Company[] companies = manager.findWithSQL(Company.class, "companyID", "SELECT " + postgresName("companyID")
                + " FROM " + postgresName(companyTableName));

        assertEquals(coolCompanyIDs.length + 1, companies.length);

        for (Company c : companies)
        {
            boolean found = false;
            for (long id : coolCompanyIDs)
            {
                if (c.getCompanyID() == id)
                {
                    found = true;
                    break;
                }
            }

            if (c.getCompanyID() == companyID)
            {
                found = true;
            }

            if (!found)
            {
                fail("Unable to find key=" + c.getCompanyID());
            }
        }

        Company company = manager.get(Company.class, companyID);
        Person[] people = manager.findWithSQL(Person.class, "id", "SELECT " + postgresName("id") + " FROM "
                + postgresName(personTableName)
                + " WHERE " + postgresName("companyID") + " = ?", company);
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
        assertEquals(coolCompanyIDs.length, manager.count(Company.class, postgresName("cool") + " = ?", true));
        assertEquals(penIDs.length, manager.count(Pen.class));
        assertEquals(1, manager.count(Person.class));
        assertEquals(0, manager.count(Select.class));
    }

    @Test(expected = RuntimeException.class)
    public void testNullTypeMapper()
    {
        EntityManager manager = EntityManagerBuilder.url("jdbc:hsqldb:mem:other_testdb").username(null).password(null)
                .auto().build();

        manager.setPolymorphicTypeMapper(null);
        manager.getPolymorphicTypeMapper();
    }
}
