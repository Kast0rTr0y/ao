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

import test.schema.Company;
import test.schema.Pen;
import test.schema.Person;
import test.schema.Profession;
import test.schema.Select;

/**
 * @author Daniel Spiewak
 */
public class EntityManagerTest extends DataTest {
	
	@Test
	public void testGetCheckID() {
		assertNull(manager.get(Person.class, personID + 1));
	}
	
	@Test
	public void testGetCache() {
		manager.get(Person.class, personID);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		manager.get(Person.class, personID);
		
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
	}
	
	@Test
	public void testReservedGet() {
		assertNull(manager.get(Select.class, 123));
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
		
		company = manager.create(Company.class, new DBParam("name", null));
		assertNull(company.getName());
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
	public void testFindCheckIDs() throws SQLException {
		Company[] coolCompanies = manager.find(Company.class, "cool = ?", true);
		
		assertEquals(coolCompanyIDs.length, coolCompanies.length);
		
		for (Company c : coolCompanies) {
			boolean found = false;
			for (long id : coolCompanyIDs) {
				if (c.getCompanyID() == id) {
					found = true;
					break;
				}
			}
			
			if (!found) {
				fail("Unable to find key=" + c.getCompanyID());
			}
		}
		
		Company[] companies = manager.find(Company.class);
		
		assertEquals(coolCompanyIDs.length + 1, companies.length);
		
		for (Company c : companies) {
			boolean found = false;
			for (long id : coolCompanyIDs) {
				if (c.getCompanyID() == id) {
					found = true;
					break;
				}
			}
			
			if (c.getCompanyID() == companyID) {
				found = true;
			}
			
			if (!found) {
				fail("Unable to find key=" + c.getCompanyID());
			}
		}
		
		Person[] people = manager.find(Person.class, "profession = ?", Profession.DEVELOPER);
		
		assertEquals(1, people.length);
		assertEquals(personID, people[0].getID());
	}
	
	@Test
	public void testFindCheckPreload() throws SQLException {
		Pen[] pens = manager.find(Pen.class);
		
		SQLLogMonitor.getInstance().markWatchSQL();
		for (Pen pen : pens) {
			pen.getWidth();
		}
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		
		SQLLogMonitor.getInstance().markWatchSQL();
		for (Pen pen : pens) {
			pen.getPerson();
		}
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
	}
	
	@Test
	public void testFindCheckDefinedPrecache() throws SQLException {
		Person[] people = manager.find(Person.class, Query.select("id,firstName,lastName"));
		
		SQLLogMonitor.getInstance().markWatchSQL();
		for (Person person : people) {
			person.getFirstName();
			person.getLastName();
		}
		assertFalse(SQLLogMonitor.getInstance().isExecutedSQL());
		
		SQLLogMonitor.getInstance().markWatchSQL();
		for (Person person : people) {
			person.getURL();
			person.getCompany();
		}
		assertTrue(SQLLogMonitor.getInstance().isExecutedSQL());
	}
	
	@Test
	public void testFindWithSQL() throws SQLException {
		Company[] coolCompanies = manager.findWithSQL(Company.class, 
				"companyID", "SELECT companyID FROM company WHERE cool = ?", true);
		
		assertEquals(coolCompanyIDs.length, coolCompanies.length);
		
		for (Company c : coolCompanies) {
			boolean found = false;
			for (long id : coolCompanyIDs) {
				if (c.getCompanyID() == id) {
					found = true;
					break;
				}
			}
			
			if (!found) {
				fail("Unable to find key=" + c.getCompanyID());
			}
		}
		
		Company[] companies = manager.findWithSQL(Company.class, "companyID", "SELECT companyID FROM company");
		
		assertEquals(coolCompanyIDs.length + 1, companies.length);
		
		for (Company c : companies) {
			boolean found = false;
			for (long id : coolCompanyIDs) {
				if (c.getCompanyID() == id) {
					found = true;
					break;
				}
			}
			
			if (c.getCompanyID() == companyID) {
				found = true;
			}
			
			if (!found) {
				fail("Unable to find key=" + c.getCompanyID());
			}
		}
	}
	
	@Test
	public void testCount() throws SQLException {
		assertEquals(coolCompanyIDs.length, manager.count(Company.class, "cool = ?", true));
		assertEquals(penIDs.length, manager.count(Pen.class));
		assertEquals(1, manager.count(Person.class));
	}
}
