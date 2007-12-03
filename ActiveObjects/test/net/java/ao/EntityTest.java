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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import test.schema.Author;
import test.schema.Book;
import test.schema.Comment;
import test.schema.Commentable;
import test.schema.Company;
import test.schema.Distribution;
import test.schema.Magazine;
import test.schema.OnlineDistribution;
import test.schema.Pen;
import test.schema.Person;
import test.schema.PersonImpl;
import test.schema.PersonLegalDefence;
import test.schema.Photo;
import test.schema.Post;

/**
 * @author Daniel Spiewak
 */
public class EntityTest extends DataTest {
	
	@Test
	public void testDatabaseAccessor() {
		Person person = manager.get(Person.class, personID);
		
		assertEquals("Daniel", person.getFirstName());
		
		assertEquals(companyID, person.getCompany().getCompanyID());
		assertEquals("Company Name", person.getCompany().getName());
		assertEquals(false, person.getCompany().isCool());
	}
	
	@Test
	public void testCacheAccessor() {
		Person person = manager.get(Person.class, personID);
		
		person.getFirstName();
		Company c = person.getCompany();
		c.getName();
		c.isCool();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		
		assertEquals("Daniel", person.getFirstName());
		
		assertEquals(companyID, person.getCompany().getCompanyID());
		assertEquals("Company Name", person.getCompany().getName());
		assertEquals(false, person.getCompany().isCool());
		
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
	}
	
	@Test
	public void testCacheMutator() throws SQLException {
		Company company = manager.create(Company.class);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		
		company.setName("Another company name");
		company.setCool(true);
		
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		assertEquals("Another company name", company.getName());
		assertEquals(true, company.isCool());
		
		company.setName(null);
		assertNull(company.getName());
		
		manager.delete(company);
	}
	
	@Test
	public void testSave() throws SQLException {
		Company company = manager.create(Company.class);
		
		company.setName("Another company name");
		company.setCool(true);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		company.save();
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
		
		String name = null;
		boolean cool = false;
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT name,cool FROM company WHERE companyID = ?");
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
		
		SQLLogMonitor.getInstance().markWatchSQL();
		company.save();
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
		
		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT name,cool FROM company WHERE companyID = ?");
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
	public void testPolymorphicMutator() throws SQLException {
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
			PreparedStatement stmt = conn.prepareStatement("SELECT commentableID,commentableType FROM comment WHERE id = ?");
			stmt.setInt(1, comment.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals(post.getID(), res.getInt(1));
				assertEquals("post", res.getString(2));
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
			PreparedStatement stmt = conn.prepareStatement("SELECT commentableID,commentableType FROM comment WHERE id = ?");
			stmt.setInt(1, comment.getID());
			
			ResultSet res = stmt.executeQuery();
			if (res.next()) {
				assertEquals(photo.getID(), res.getInt(1));
				assertEquals("photo", res.getString(2));
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
			PreparedStatement stmt = conn.prepareStatement("SELECT commentableID,commentableType FROM comment WHERE id = ?");
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
	public void testCreate() throws SQLException {
		SQLLogMonitor.getInstance().markWatchSQL();
		Company company = manager.create(Company.class);
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
		
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT companyID FROM company WHERE companyID = ?");
			stmt.setLong(1, company.getCompanyID());
			
			ResultSet res = stmt.executeQuery();
			if (!res.next()) {
				fail("Unable to find INSERTed company row");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(company);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		Person person = manager.create(Person.class, new DBParam("url", "http://www.codecommit.com"));
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
		
		conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT id FROM person WHERE id = ?");
			stmt.setInt(1, person.getID());
			
			ResultSet res = stmt.executeQuery();
			if (!res.next()) {
				fail("Unable to find INSERTed person row");
			}
			res.close();
			stmt.close();
		} finally {
			conn.close();
		}
		
		manager.delete(person);
	}
	
	@Test
	public void testStringGenerate() throws SQLException {
		Company company = manager.create(Company.class);
		
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT motivation FROM company WHERE companyID = ?");
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
	public void testDelete() throws SQLException {
		Company company = manager.create(Company.class);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		manager.delete(company);
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
		
		Connection conn = manager.getProvider().getConnection();
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT companyID FROM company WHERE companyID = ?");
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
	public void testDefinedImplementation() {
		Person person = manager.get(Person.class, personID);
		
		PersonImpl.enableOverride = true;
		
		SQLLogMonitor.getInstance().markWatchSQL();
		assertEquals("Smith", person.getLastName());
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		
		PersonImpl.enableOverride = false;
	}
	
	// if this test doesn't stack overflow, we're good
	@Test
	public void testDefinedImplementationRecursion() {
		Person person = manager.get(Person.class, personID);
		person.setLastName("Jameson");
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
	public void testOneToManyRetrievalPreload() {
		manager.getRelationsCache().flush();
		
		Person person = manager.get(Person.class, personID);
		
		for (Pen pen : person.getPens()) {
			SQLLogMonitor.getInstance().markWatchSQL();
			pen.getWidth();
			assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		}
	}
	
	@Test
	public void testOneToManyRetrievalFromCache() {
		Person person = manager.get(Person.class, personID);
		person.getPens();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		person.getPens();
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
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
	public void testManyToManyRetrievalPreload() {
		manager.getRelationsCache().flush();
		
		Person person = manager.get(Person.class, personID);
		
		for (PersonLegalDefence defence : person.getPersonLegalDefences()) {
			SQLLogMonitor.getInstance().markWatchSQL();
			defence.getSeverity();
			assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		}
	}
	
	@Test
	public void testManyToManyRetrievalFromCache() {
		Person person = manager.get(Person.class, personID);
		person.getPersonLegalDefences();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		person.getPersonLegalDefences();
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
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
	public void testPolymorphicOneToManyRetrievalPreload() {
		manager.getRelationsCache().flush();
		
		Post post = manager.get(Post.class, postID);
		
		for (Comment comment : post.getComments()) {
			SQLLogMonitor.getInstance().markWatchSQL();
			comment.getTitle();
			assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		}
	}

	@Test
	public void testPolymorphicOneToManyRetrievalFromCache() {
		Post post = manager.get(Post.class, personID);
		post.getComments();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		post.getComments();
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
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
					for (int o = 0; o < bookDistributionIDs[o].length; o++) {
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
					for (int o = 0; o < magazineDistributionIDs[o].length; o++) {
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
	public void testPolymorphicManyToManyRetrievalPreload() {
		manager.getRelationsCache().flush();
		
		Book book = manager.get(Book.class, bookIDs[0]);
		
		for (Author author : book.getAuthors()) {
			SQLLogMonitor.getInstance().markWatchSQL();
			author.getName();
			assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		}
		
		for (Distribution distribution : book.getDistributions()) {
			if (distribution instanceof OnlineDistribution) {
				SQLLogMonitor.getInstance().markWatchSQL();
				((OnlineDistribution) distribution).getURL();
				assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
			}
		}
	}

	@Test
	public void testPolymorphicManyToManyRetrievalFromCache() {
		Magazine magazine = manager.get(Magazine.class, magazineIDs[0]);
		magazine.getAuthors();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		magazine.getAuthors();
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		
		magazine.getDistributions();
		
		SQLLogMonitor.getInstance().markWatchSQL();
		magazine.getDistributions();
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
	}
}
