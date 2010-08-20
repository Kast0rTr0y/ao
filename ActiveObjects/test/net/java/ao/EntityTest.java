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

import net.java.ao.test.config.JdbcConfiguration;
import org.junit.Test;
import test.schema.Author;
import test.schema.Authorship;
import test.schema.Book;
import test.schema.Comment;
import test.schema.Commentable;
import test.schema.Company;
import test.schema.Distribution;
import test.schema.EmailAddress;
import test.schema.Magazine;
import test.schema.Message;
import test.schema.OnlineDistribution;
import test.schema.Pen;
import test.schema.Person;
import test.schema.PersonImpl;
import test.schema.PersonLegalDefence;
import test.schema.PersonSuit;
import test.schema.Photo;
import test.schema.Post;
import test.schema.PrintDistribution;
import test.schema.Profession;
import test.schema.PublicationToDistribution;
import test.schema.Select;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * @author Daniel Spiewak
 */
public class EntityTest extends DataTest {

	@Test
	public void testGetEntityManager() {
		assertEquals(manager, manager.get(Company.class, companyID).getEntityManager());
		assertEquals(manager, manager.get(Person.class, personID).getEntityManager());
	}
	
	@Test
	public void testToString() throws SQLException {
		String companyTableName = manager.getTableNameConverter().getName(Company.class);
		String personTableName = manager.getTableNameConverter().getName(Person.class);
		String selectTableName = manager.getTableNameConverter().getName(Select.class);
		
		assertEquals(personTableName + " {id = " + personID + "}", 
				manager.get(Person.class, personID).toString());
		
		assertEquals(companyTableName + " {companyID = " + companyID + "}", 
				manager.get(Company.class, companyID).toString());
		
		Select select = manager.create(Select.class);
		assertEquals(selectTableName + " {id = " + select.getID() + "}", select.toString());
		manager.delete(select);
	}

	@Test
	public void testDatabaseAccessor() {
		Person person = manager.get(Person.class, personID);
		
		assertEquals("Daniel", person.getFirstName());
		assertEquals(Profession.DEVELOPER, person.getProfession());
		
		assertEquals(companyID, person.getCompany().getCompanyID());
		assertEquals("Company Name", person.getCompany().getName());
		assertEquals(false, person.getCompany().isCool());
	}

    @Test
    public void testCacheAccessor() throws Exception
    {
        final Person person = manager.get(Person.class, personID);
        person.getFirstName();

        final Company c = person.getCompany();
        c.getName();
        c.isCool();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                assertEquals("Daniel", person.getFirstName());

                assertEquals(companyID, person.getCompany().getCompanyID());
                assertEquals("Company Name", person.getCompany().getName());
                assertEquals(false, person.getCompany().isCool());
                return null;
            }
        });
    }

    @Test
    public void testUncachableCacheAccessor() throws Exception
    {
        final Company company = manager.get(Company.class, companyID);
        company.getImage().close();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                company.getImage().close();
                return null;
            }
        });
    }

    @Test
	public void testBlobAccessor() throws IOException, SQLException {
		Person person = manager.get(Person.class, personID);
		byte[] image = person.getImage();
		
		assertEquals(13510, image.length);
		
		Company company = manager.get(Company.class, companyID);
		InputStream is = company.getImage();
	
		int count = 0;
		try {
			while (is.read() >= 0) {
				count++;
			}
			is.close();
		} catch (IOException e) {
			throw new SQLException(e.getMessage());
		}
	
		assertEquals(13510, count);
	}

	@Test
	public void testPolymorphicAccessor() throws SQLException {
		Comment comment = manager.get(Comment.class, postCommentIDs[0]);
		Commentable commentable = comment.getCommentable();
		
		assertTrue(commentable instanceof Post);
		assertEquals(postID, commentable.getID());
		
		comment = manager.get(Comment.class, photoCommentIDs[0]);
		commentable = comment.getCommentable();
		
		assertTrue(commentable instanceof Photo);
		assertEquals(photoID, commentable.getID());
		
		comment = manager.create(Comment.class);
		assertNull(comment.getCommentable());
		manager.delete(comment);
	}

	@Test
	public void testCacheMutator() throws Exception
    {
		final Company company = manager.create(Company.class);

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                company.setName("Another company name");
                company.setCool(true);
                return null;
            }
        });

        assertEquals("Another company name", company.getName());
		assertEquals(true, company.isCool());
		
		company.setName(null);
		assertNull(company.getName());
		
		manager.delete(company);
	}
	
	@Test
	public void testSave() throws Exception
    {
		String companyTableName = manager.getTableNameConverter().getName(Company.class);
		companyTableName = manager.getProvider().processID(companyTableName);

		String personTableName = manager.getTableNameConverter().getName(Person.class);
		personTableName = manager.getProvider().processID(personTableName);
		
		final Company company = manager.create(Company.class);
		
		company.setName("Another company name");
		company.setCool(true);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                company.save();
                return null;
            }
        });

		String name = null;
		boolean cool = false;
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("name")
					+ ", " + processId("cool") + " FROM " + companyTableName
					+ " WHERE " + processId("companyID") + " = ?");
			stmt.setLong(1, company.getCompanyID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				name = res.getString("name");
				cool = res.getBoolean("cool");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		assertEquals("Another company name", name);
		assertEquals(true, cool);
		
		company.setName(null);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                company.save();
                return null;
            }
        });

		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("name")
					+ ", " + processId("cool") + " FROM " + companyTableName
					+ " WHERE " + processId("companyID") + " = ?");
			stmt.setLong(1, company.getCompanyID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				name = res.getString("name");
				cool = res.getBoolean("cool");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		assertNull(name);
		assertEquals(true, cool);
		
		manager.delete(company);
		
		final Person person = manager.get(Person.class, personID);
		person.setProfession(Profession.MUSICIAN);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.save();
                return null;
            }
        });

		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " 
					+ processId("profession") + " FROM " + personTableName
					+ " WHERE " + processId("id") + " = ?");
			stmt.setInt(1, person.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals(1, res.getInt("profession"));
			} else {
				fail("No person found");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		person.setProfession(Profession.DEVELOPER);
		person.save();
	}
	
	@Test
	public void testTransientCacheAccessor() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.setAge(25);
		person.save();
		
		person.getAge();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                assertEquals(25, person.getAge());
                return null;
            }
        });
	}
	
	@Test
	public void testTransientCacheMutator() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.setAge(25);
		person.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                assertEquals(25, person.getAge());
                return null;
            }
        });
	}

	@Test
	public void testBlobMutator() throws IOException {
		Person person = manager.get(Person.class, personID);
		InputStream is = EntityTest.class.getResourceAsStream("/icon.png");
		byte[] image = new byte[is.available()];
		
		int i = 0;
		int b = 0;
		while ((b = is.read()) >= 0) {
			image[i++] = (byte) b;
		}
		is.close();
		
		person.setImage(image);
		person.save();
		
		Company company = manager.get(Company.class, companyID);
		
		is = EntityTest.class.getResourceAsStream("/icon.png");
		company.setImage(is);
		company.save();
		
		is.close();
	}
	
	@Test
	public void testPolymorphicMutator() throws SQLException {
		String commentTableName = manager.getTableNameConverter().getName(Comment.class);
		commentTableName = manager.getProvider().processID(commentTableName);
		
		Post post = manager.create(Post.class);
		post.setTitle("My Temp Test Title");
		post.save();
		
		Comment comment = manager.create(Comment.class);
		comment.setTitle("My Temp Test Comment");
		comment.setText("Here's some test text");
		comment.setCommentable(post);
		comment.save();
		
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("commentableID") + ", "
					+ processId("commentableType") + " FROM "
					+ commentTableName + " WHERE " + processId("id") + " = ?");
			stmt.setInt(1, comment.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals(post.getID(), res.getInt(1));
				assertEquals("post", res.getString(2).toLowerCase());
			} else {
				fail("No results found");
			}
			res.close();
			
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(post);
		
		Photo photo = manager.create(Photo.class);
		
		comment.setCommentable(photo);
		comment.save();
		
		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("commentableID")
					+ ", " + processId("commentableType") + " FROM "
					+ commentTableName + " WHERE " + processId("id") + " = ?");
			stmt.setInt(1, comment.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals(photo.getID(), res.getInt(1));
				assertEquals("photo", res.getString(2).toLowerCase());
			} else {
				fail("No results found");
			}
			res.close();
			
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(photo);
		
		comment.setCommentable(null);
		comment.save();
		
		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("commentableID")
					+ ", " + processId("commentableType") + " FROM "
					+ commentTableName + " WHERE id = ?");
			stmt.setInt(1, comment.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertNull(res.getString(1));
				assertNull(res.getString(2));
			} else {
				fail("No results found");
			}
			res.close();
			
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(comment);
	}
	
	@Test
	public void testPropertyChangeListener() throws SQLException {
		Person person = manager.create(Person.class, new DBParam("url", "http://stackoverflow.com"));
		
		final String[] fired = {""};
		final String[] value = {""};
		
		person.addPropertyChangeListener(new PropertyChangeListener() {
			public void propertyChange(PropertyChangeEvent evt) {
				fired[0] = evt.getPropertyName();
				value[0] = (String) evt.getNewValue();
			}
		});
		
		person.setFirstName("Daniel");
		assertEquals("", fired[0]);
		assertEquals("", value[0]);
		
		person.save();
		assertEquals("firstName", fired[0]);
		assertEquals("Daniel", value[0]);
		
		fired[0] = value[0] = "";
		
		person.setFirstName("Chris");
		assertEquals("", fired[0]);
		assertEquals("", value[0]);
		
		manager.delete(person);
	}
	
	@Test
	public void testOnUpdate() throws Exception
    {
		if (JdbcConfiguration.get().getUrl().trim().startsWith("jdbc:hsqldb")) {
			return;		// hsqldb doesn't support @OnUpdate
		}
		
		final Person person = manager.get(Person.class, personID);
		person.setFirstName(person.getFirstName());		// no-op to guarentee value
		person.save();
		
		final Calendar old = person.getModified();
		person.getAge();
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
		
		person.setAge(2);
		person.save();
		
		assertNotNull(old);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                assertFalse(old.getTimeInMillis() == person.getModified().getTimeInMillis());
                return null;
            }
        });

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getModified();
                return null;
            }
        });
	}
	
	@Test
	public void testAccessNullPrimitive() throws MalformedURLException, SQLException {
		Company company = manager.create(Company.class);
		assertFalse(company.isCool());		// if we get NPE, we're in trouble
		
		manager.delete(company);
	}
	
	@Test
	public void testMutateNull() throws SQLException {
		Company company = manager.create(Company.class);
		
		company.setName(null);
		company.save();
		
		manager.flush(company);
		assertEquals(null, company.getName());
		
		company.setAddressInfo(null);
		company.save();
		
		manager.flush(company);
		assertEquals(null, company.getAddressInfo());
		
		manager.delete(company);
	}
	
	@Test
	public void testConcurrency() throws Throwable {
		Thread[] threads = new Thread[50];
		final Throwable[] exceptions = new Throwable[threads.length];
		
		final Person person = manager.create(Person.class, new DBParam("url", new URL("http://www.howtogeek.com")));
		final Company company = manager.create(Company.class);
		
		for (int i = 0; i < threads.length; i++) {
			final int threadNum = i;
			threads[i] = new Thread("Concurrency Test " + i) {
				@Override
				public void run() {		// a fair-few interleved instructions
					try {
						person.setAge(threadNum);
						person.save();
						
						company.setName(getName());
						company.save();
						
						company.getName();
						assertFalse(company.isCool());
						
						person.setFirstName(getName());
						person.setLastName("Spiewak");
						person.setCompany(company);
						person.save();
						
						person.getNose();
						person.getFirstName();
						person.getAge();
						
						person.setFirstName("Daniel");
						person.save();
						
						company.getImage();
						
						person.getFavoriteClass();
					} catch (Throwable t) {
						exceptions[threadNum] = t;
					}
				}
			};
		}
		
		for (Thread thread : threads) {
			thread.start();
		}
		
		for (Thread thread : threads) {
			thread.join();
		}
		
		manager.delete(person);
		manager.delete(company);
		
		for (Throwable e : exceptions) {
			if (e != null) {
				throw e;
			}
		}
	}

	@Test
	public void testReservedOperations() throws SQLException {
		Select select = manager.create(Select.class);
		select.setWhere("Some test criteria");
		select.setAnd(false);
		select.save();
		
		manager.flush(select);
		
		assertEquals("Some test criteria", select.getWhere());
		assertFalse(select.isAnd());
		
		manager.delete(select);
	}
	
	@Test
	public void testStringGenerate() throws SQLException {
		String companyTableName = manager.getTableNameConverter().getName(Company.class);
		companyTableName = manager.getProvider().processID(companyTableName);
		
		Company company = manager.create(Company.class);
		
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("motivation") + " FROM "
					+ companyTableName 
					+ " WHERE " + processId("companyID") + " = ?");
			stmt.setLong(1, company.getCompanyID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals("Work smarter, not harder", res.getString("motivation"));
			} else {
				fail("Unable to find INSERTed company row");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(company);
	}
	
	@Test
	public void testDelete() throws Exception
    {
		String companyTableName = manager.getTableNameConverter().getName(Company.class);
		companyTableName = manager.getProvider().processID(companyTableName);

        final Company company = manager.create(Company.class);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                manager.delete(company);
                return null;
            }
        });

        Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT " + processId("companyID")
					+ " FROM " + companyTableName 
					+ " WHERE " + processId("companyID") + " = ?");
			stmt.setLong(1, company.getCompanyID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				fail("Row was not deleted");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
	}
	
	@Test
	public void testDefinedImplementation() throws Exception
    {
        final Person person = manager.get(Person.class, personID);

        PersonImpl.enableOverride = true;

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                assertEquals("Smith", person.getLastName());
                return null;
            }
        });

        PersonImpl.enableOverride = false;
    }
	
	// if this test doesn't stack overflow, we're good
	@Test
	public void testDefinedImplementationRecursion() {
		Person person = manager.get(Person.class, personID);
		
		try {
			person.setLastName("Jameson");
		} catch (StackOverflowError e) {
			assertTrue(false);
		}
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testNotNullSoftCheck() {
		Message message = manager.get(Message.class, messageIDs[0]);
		message.setContents(null);		// should throw exception
		
		manager.flush(message);
	}
	
	@Test
	public void testOneToOneRetrievalID() {
		Person person = manager.get(Person.class, personID);
		assertEquals(noseID, person.getNose().getID());
	}
	
	@Test
	public void testOneToManyRetrievalIDs() {
		EntityProxy.ignorePreload = true;
		try {
			Person person = manager.get(Person.class, personID);
			Pen[] pens = person.getPens();

			assertEquals(penIDs.length, pens.length);

			for (Pen pen : pens) {
				boolean found = false;
				for (int id : penIDs) {
					if (pen.getID() == id) {
						found = true;
						break;
					}
				}

				if (!found) {
					fail("Unable to find id=" + pen.getID());
				}
			}
		} finally {
			EntityProxy.ignorePreload = false;
		}
	}

    @Test
    public void testOneToManyRetrievalPreload() throws Exception
    {
        manager.getRelationsCache().flush();

        Person person = manager.get(Person.class, personID);

        for (final Pen pen : person.getPens())
        {
            sql.checkNotExecuted(new Command<Void>()
            {
                public Void run() throws Exception
                {
                    pen.getWidth();
                    return null;
                }
            });
        }
    }

    @Test
    public void testOneToManyRetrievalFromCache() throws Exception
    {
        final Person person = manager.get(Person.class, personID);
        person.getPens();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });
    }

    @Test
	public void testOneToManyCacheExpiry() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.getPens();
		
		Pen pen = manager.create(Pen.class);
		pen.setPerson(person);
		pen.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });

		manager.delete(pen);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });

		pen = manager.create(Pen.class, new DBParam("personID", person));

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });

		pen.setPerson(null);
		pen.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });

        manager.delete(pen);
	}
	
	@Test
	public void testOneToManyCacheInvalidation() throws SQLException {
		Person p = manager.create(Person.class, new HashMap<String, Object>() {{put("url", "test-url");}});
		Company c = manager.create(Company.class);
		
		c.getPeople();
		
		p.setCompany(c);
		p.save();
		
		Person[] people = c.getPeople();
		assertEquals(people.length, 1);
		assertEquals(people[0], p);

        manager.delete(p);
        manager.delete(c);
	}
	
	@Test
	public void testManyToManyRetrievalIDs() {
		EntityProxy.ignorePreload = true;
		try {
			Person person = manager.get(Person.class, personID);
			PersonLegalDefence[] defences = person.getPersonLegalDefences();

			assertEquals(defenceIDs.length, defences.length);

			for (PersonLegalDefence defence : defences) {
				boolean found = false;
				for (int id : defenceIDs) {
					if (defence.getID() == id) {
						found = true;
						break;
					}
				}

				if (!found) {
					fail("Unable to find id=" + defence.getID());
				}
			}
		} finally {		
			EntityProxy.ignorePreload = false;
		}
	}
	
	@Test
	public void testManyToManyRetrievalPreload() throws Exception
    {
		manager.getRelationsCache().flush();
		
		Person person = manager.get(Person.class, personID);

        for (final PersonLegalDefence defence : person.getPersonLegalDefences())
        {
            sql.checkNotExecuted(new Command<Void>()
            {
                public Void run() throws Exception
                {
                    defence.getSeverity();
                    return null;
                }
            });
        }
    }
	
	@Test
	public void testManyToManyRetrievalFromCache() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.getPersonLegalDefences();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });
    }
	
	@Test
	public void testManyToManyCacheExpiry() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.getPersonLegalDefences();
		
		PersonSuit suit = manager.create(PersonSuit.class);
		suit.setPerson(person);
		suit.setPersonLegalDefence(manager.get(PersonLegalDefence.class, defenceIDs[0]));
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

		manager.delete(suit);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        suit = manager.create(PersonSuit.class, new DBParam("personID", person),
				new DBParam("personLegalDefenceID", defenceIDs[1]));

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

		suit.setPerson(null);
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        suit.setPerson(person);
		suit.save();
		
		person.getPersonLegalDefences();
		
		suit.setPersonLegalDefence(null);
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        PersonLegalDefence defence = manager.create(PersonLegalDefence.class);
		
		suit.setPersonLegalDefence(defence);
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

		suit.setPersonLegalDefence(null);
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

		manager.delete(suit);
		manager.delete(defence);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });
    }

	@Test
	public void testPolymorphicOneToManyRetrievalIDs() {
		EntityProxy.ignorePreload = true;
		try {
			Post post = manager.get(Post.class, postID);
			Comment[] comments = post.getComments();

			assertEquals(postCommentIDs.length, comments.length);

			for (Comment comment : comments) {
				boolean found = false;
				for (int id : postCommentIDs) {
					if (comment.getID() == id) {
						found = true;
						break;
					}
				}

				if (!found) {
					fail("Unable to find id=" + comment.getID());
				}
			}

			Photo photo = manager.get(Photo.class, photoID);
			comments = photo.getComments();

			assertEquals(photoCommentIDs.length, comments.length);

			for (Comment comment : comments) {
				boolean found = false;
				for (int id : photoCommentIDs) {
					if (comment.getID() == id) {
						found = true;
						break;
					}
				}

				if (!found) {
					fail("Unable to find id=" + comment.getID());
				}
			}
		} finally {
			EntityProxy.ignorePreload = false;
		}
	}
	
	@Test
	public void testPolymorphicOneToManyRetrievalPreload() throws Exception
    {
		manager.getRelationsCache().flush();
		
		Post post = manager.get(Post.class, postID);

        for (final Comment comment : post.getComments())
        {
            sql.checkNotExecuted(new Command<Void>()
            {
                public Void run() throws Exception
                {
                    comment.getTitle();
                    return null;
                }
            });
        }
    }

	@Test
	public void testPolymorphicOneToManyRetrievalFromCache() throws Exception
    {
		final Post post = manager.get(Post.class, postID);
		post.getComments();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                post.getComments();
                return null;
            }
        });
	}
	
	@Test
	public void testPolymorphicOneToManyCacheExpiry() throws Exception
    {
		final Post post = manager.get(Post.class, postID);
		post.getComments();
		
		Comment comment = manager.create(Comment.class);
		comment.setCommentable(post);
		comment.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                post.getComments();
                return null;
            }
        });

		manager.delete(comment);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                post.getComments();
                return null;
            }
        });

		comment = manager.create(Comment.class, new DBParam("commentableID", post), 
				new DBParam("commentableType", manager.getPolymorphicTypeMapper().convert(Post.class)));

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                post.getComments();
                return null;
            }
        });

		comment.setCommentable(null);
		comment.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                post.getComments();
                return null;
            }
        });

        manager.delete(comment);
	}

	@Test
	public void testPolymorphicManyToManyRetrievalIDs() {
		EntityProxy.ignorePreload = true;
		try {
			for (int i = 0; i < bookIDs.length; i++) {
				Book book = manager.get(Book.class, bookIDs[i]);
				Author[] authors = book.getAuthors();

				assertEquals(bookAuthorIDs[i].length, authors.length);

				for (Author author : authors) {
					boolean found = false;
					for (int id : bookAuthorIDs[i]) {
						if (author.getID() == id) {
							found = true;
							break;
						}
					}

					if (!found) {
						fail("Unable to find id=" + author.getID());
					}
				}
			}
			
			for (int i = 0; i < magazineIDs.length; i++) {
				Magazine magazine = manager.get(Magazine.class, magazineIDs[i]);
				Author[] authors = magazine.getAuthors();

				assertEquals(magazineAuthorIDs[i].length, authors.length);

				for (Author author : authors) {
					boolean found = false;
					for (int id : magazineAuthorIDs[i]) {
						if (author.getID() == id) {
							found = true;
							break;
						}
					}

					if (!found) {
						fail("Unable to find id=" + author.getID());
					}
				}
			}

			for (int i = 0; i < bookIDs.length; i++) {
				Book book = manager.get(Book.class, bookIDs[i]);
				Distribution[] distributions = book.getDistributions();

				assertEquals(bookDistributionIDs[i].length, distributions.length);

				for (Distribution distribution : distributions) {
					boolean found = false;
					for (int o = 0; o < bookDistributionIDs[i].length; o++) {
						if (distribution.getID() == bookDistributionIDs[i][o] 
								&& distribution.getEntityType().equals(bookDistributionTypes[i][o])) {
							found = true;
							break;
						}
					}

					if (!found) {
						fail("Unable to find id=" + distribution.getID() 
								+ ", type=" + manager.getPolymorphicTypeMapper().convert(distribution.getEntityType()));
					}
				}
			}

			for (int i = 0; i < magazineIDs.length; i++) {
				Magazine magazine = manager.get(Magazine.class, magazineIDs[i]);
				Distribution[] distributions = magazine.getDistributions();

				assertEquals(magazineDistributionIDs[i].length, distributions.length);

				for (Distribution distribution : distributions) {
					boolean found = false;
					for (int o = 0; o < magazineDistributionIDs[i].length; o++) {
						if (distribution.getID() == magazineDistributionIDs[i][o] 
								&& distribution.getEntityType().equals(magazineDistributionTypes[i][o])) {
							found = true;
							break;
						}
					}

					if (!found) {
						fail("Unable to find id=" + distribution.getID() 
								+ ", type=" + manager.getPolymorphicTypeMapper().convert(distribution.getEntityType()));
					}
				}
			}
		} finally {		
			EntityProxy.ignorePreload = false;
		}
	}
	
	@Test
	public void testPolymorphicManyToManyRetrievalPreload() throws Exception
    {
        manager.getRelationsCache().flush();

        Book book = manager.get(Book.class, bookIDs[0]);

        for (final Author author : book.getAuthors())
        {
            sql.checkNotExecuted(new Command<Void>()
            {
                public Void run() throws Exception
                {
                    author.getName();
                    return null;
                }
            });
        }
    }

	@Test
	public void testPolymorphicManyToManyRetrievalFromCache() throws Exception
    {
		final Magazine magazine = manager.get(Magazine.class, magazineIDs[0]);
		magazine.getAuthors();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

        magazine.getDistributions();

        sql.checkNotExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });
	}
	
	@Test
	public void testPolymorphicManyToManyCacheExpiry() throws Exception
    {
		final Magazine magazine = manager.get(Magazine.class, magazineIDs[0]);
		magazine.getAuthors();
		
		Authorship authorship = manager.create(Authorship.class);
		authorship.setPublication(magazine);
		authorship.setAuthor(manager.get(Author.class, magazineAuthorIDs[0][0]));
		authorship.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		manager.delete(authorship);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		authorship = manager.create(Authorship.class, new DBParam("publicationID", magazine), 
				new DBParam("publicationType", manager.getPolymorphicTypeMapper().convert(Magazine.class)),
				new DBParam("authorID", bookAuthorIDs[0][1]));

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		authorship.setAuthor(null);
		authorship.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		authorship.setPublication(magazine);
		authorship.save();
		
		magazine.getAuthors();
		
		Author author = manager.create(Author.class);
		
		authorship.setAuthor(author);
		authorship.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		manager.delete(authorship);
		manager.delete(author);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getAuthors();
                return null;
            }
        });

		magazine.getDistributions();
		
		PublicationToDistribution mapping = manager.create(PublicationToDistribution.class);
		mapping.setPublication(magazine);
		mapping.setDistribution(manager.get(OnlineDistribution.class, magazineDistributionIDs[0][1]));
		mapping.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });

		manager.delete(mapping);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });

		mapping = manager.create(PublicationToDistribution.class);
		mapping.setPublication(magazine);
		mapping.setDistribution(manager.get(OnlineDistribution.class, magazineDistributionIDs[0][1]));
		mapping.save();
		
		magazine.getDistributions();
		
		mapping.setDistribution(null);
		mapping.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });

		mapping.setDistribution(manager.get(PrintDistribution.class, magazineDistributionIDs[0][0]));
		mapping.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });

		mapping.setPublication(null);
		mapping.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });

        manager.delete(mapping);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                magazine.getDistributions();
                return null;
            }
        });
    }

	@Test
	public void testMultiPathPolymorphicOneToManyRetrievalIDs() {
		EntityProxy.ignorePreload = true;
		try {
			EmailAddress address = manager.get(EmailAddress.class, addressIDs[0]);
			Message[] messages = address.getMessages();

			assertEquals(messageIDs.length, messages.length);

			for (Message message : messages) {
				boolean found = false;
				for (int id : messageIDs) {
					if (message.getID() == id) {
						found = true;
						break;
					}
				}

				if (!found) {
					fail("Unable to find id=" + message.getID());
				}
			}
		} finally {
			EntityProxy.ignorePreload = false;
		}
	}
}
