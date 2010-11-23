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

import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.config.DynamicJdbcConfiguration;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.lucene.WithIndex;
import org.apache.lucene.index.IndexReader;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Daniel Spiewak
 */
@Data(DatabaseProcessor.class)
@Jdbc(DynamicJdbcConfiguration.class)
@WithIndex
public class SearchTest extends ActiveObjectsIntegrationTest
{
    @Before
    public void setUp() throws Exception
    {
        Map<String, String[]> people = new LinkedHashMap<String, String[]>()
        {
            {
                put("Daniel", new String[]{"Spiewak", "http://www.codecommit.com"});
                put("Christopher", new String[]{"Spiewak", "http://www.weirdthings.com"});

                put("Jack", new String[]{"O'Neil", "http://www.gateworld.net"});
                put("Katheryn", new String[]{"Janeway", "http://www.startrek.com"});

                put("Martin", new String[]{"Smith", "http://www.delirious.com"});
                put("Steve", new String[]{"Smith", "http://www.panthers.com"});
                put("Craig", new String[]{"Antler", "http://www.rockyandbullwinkle.com"});
                put("Greg", new String[]{"Smith", "http://www.imagination.com"});
            }
        };
        String[] companies = {"Miller", "Chrysler", "Apple", "GM", "HP", "Dell", "Wal-Mart",
                "Ma Bell", "Sell, Sell, Sell", "Viacom"};

        for (String firstName : people.keySet())
        {
            Person person = entityManager.create(Person.class, new DBParam("url", people.get(firstName)[1]));
            person.setFirstName(firstName);
            person.setLastName(people.get(firstName)[0]);
            person.save();
        }

        for (String name : companies)
        {
            Company company = entityManager.create(Company.class);
            company.setName(name);
            company.save();
        }
    }

    @Test
    public void testSearchable()
    {
        List<String> fields = Common.getSearchableFields(entityManager, Company.class);

        assertEquals(1, fields.size());
        assertEquals("name", fields.get(0));

        List<String> possibilities = Arrays.asList(new String[]{"firstName", "lastName"});
        fields = Common.getSearchableFields(entityManager, Person.class);

        assertEquals(possibilities.size(), fields.size());

        Collections.sort(fields);
        for (int i = 0; i < possibilities.size(); i++)
        {
            assertEquals(possibilities.get(i), fields.get(i));
        }
    }

    @Test
    public void testSearch() throws Exception
    {
        // using the searchable
        final SearchableEntityManager entityManager = getSearchableEntityManager();

        Person[] people = entityManager.search(Person.class, "Spiewak");
        Map<String, String> resultsMap = new HashMap<String, String>()
        {
            {
                put("Daniel", "Spiewak");
                put("Christopher", "Spiewak");
            }
        };

        assertEquals(resultsMap.size(), people.length);
        for (Person p : people)
        {
            assertNotNull(resultsMap.get(p.getFirstName()));
            assertEquals(resultsMap.get(p.getFirstName()), p.getLastName());

            resultsMap.remove(p.getFirstName());
        }
        assertEquals(0, resultsMap.size());

        people = entityManager.search(Person.class, "martin");
        resultsMap = new HashMap<String, String>()
        {
            {
                put("Martin", "Smith");
            }
        };

        assertEquals(resultsMap.size(), people.length);
        for (Person p : people)
        {
            assertNotNull(resultsMap.get(p.getFirstName()));
            assertEquals(resultsMap.get(p.getFirstName()), p.getLastName());

            resultsMap.remove(p.getFirstName());
        }
        assertEquals(0, resultsMap.size());

        people = entityManager.search(Person.class, "sMitH");
        resultsMap = new HashMap<String, String>()
        {
            {
                put("Martin", "Smith");
                put("Steve", "Smith");
                put("Greg", "Smith");
            }
        };

        assertEquals(resultsMap.size(), people.length);
        for (Person p : people)
        {
            assertNotNull(resultsMap.get(p.getFirstName()));
            assertEquals(resultsMap.get(p.getFirstName()), p.getLastName());

            resultsMap.remove(p.getFirstName());
        }
        assertEquals(0, resultsMap.size());

        Company[] companies = entityManager.search(Company.class, "miller");
        Set<String> resultSet = new HashSet<String>()
        {
            {
                add("Miller");
            }
        };

        assertEquals(resultSet.size(), companies.length);
        for (Company c : companies)
        {
            assertTrue(resultSet.contains(c.getName()));
            resultSet.remove(c.getName());
        }
        assertEquals(0, resultSet.size());

        companies = entityManager.search(Company.class, "deLL sell");
        resultSet = new HashSet<String>()
        {
            {
                add("Dell");
                add("Sell, Sell, Sell");
            }
        };

        assertEquals(resultSet.size(), companies.length);
        for (Company c : companies)
        {
            assertTrue(resultSet.contains(c.getName()));
            resultSet.remove(c.getName());
        }
        assertEquals(0, resultSet.size());

        companies = entityManager.search(Company.class, "vaguesearchofnothingatall");
        resultSet = new HashSet<String>();

        assertEquals(resultSet.size(), companies.length);
        for (Company c : companies)
        {
            assertTrue(resultSet.contains(c.getName()));
            resultSet.remove(c.getName());
        }
        assertEquals(0, resultSet.size());
    }

    @Test
    public void testDelete() throws Exception
    {
        final SearchableEntityManager entityManager = getSearchableEntityManager();

        assertEquals(0, entityManager.search(Person.class, "foreman").length);

        Person person = entityManager.create(Person.class, new DBParam("url", "http://en.wikipedia.org"));
        person.setFirstName("George");
        person.setLastName("Foreman");
        person.save();

        assertEquals(1, entityManager.search(Person.class, "foreman").length);

        entityManager.delete(person);

        assertEquals(0, entityManager.search(Person.class, "foreman").length);
    }

    @Test
    public void testAddToIndex() throws Exception
    {
        final SearchableEntityManager entityManager = getSearchableEntityManager();

        assertEquals(0, entityManager.search(Person.class, "foreman").length);

        Person person = entityManager.create(Person.class, new DBParam("url", "http://en.wikipedia.org"));
        person.setFirstName("George");
        person.setLastName("Foreman");
        person.save();

        assertEquals(1, entityManager.search(Person.class, "foreman").length);

        entityManager.removeFromIndex(person);
        assertEquals(0, entityManager.search(Person.class, "foreman").length);

        entityManager.addToIndex(person);
        assertEquals(1, entityManager.search(Person.class, "foreman").length);

        entityManager.delete(person);
        assertEquals(0, entityManager.search(Person.class, "foreman").length);
    }

    @Test
    public void testRemoveFromIndex() throws Exception
    {
        final SearchableEntityManager entityManager = getSearchableEntityManager();

        assertEquals(0, entityManager.search(Person.class, "foreman").length);

        Person person = entityManager.create(Person.class, new DBParam("url", "http://en.wikipedia.org"));
        person.setFirstName("George");
        person.setLastName("Foreman");
        person.save();

        assertEquals(1, entityManager.search(Person.class, "foreman").length);

        entityManager.removeFromIndex(person);
        assertEquals(0, entityManager.search(Person.class, "foreman").length);

        entityManager.delete(person);
        assertEquals(0, entityManager.search(Person.class, "foreman").length);
    }

    @Test
    public void testOptimize() throws Exception
    {
        final SearchableEntityManager entityManager = getSearchableEntityManager();

        IndexReader reader = IndexReader.open(entityManager.getIndexDir());
        assertFalse(reader.isOptimized());
        reader.close();

        entityManager.optimize();

        reader = IndexReader.open(entityManager.getIndexDir());
        assertTrue(reader.isOptimized());
        reader.close();
    }

    private SearchableEntityManager getSearchableEntityManager()
    {
        return (SearchableEntityManager) entityManager;
    }
}
