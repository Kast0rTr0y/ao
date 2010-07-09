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

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import org.junit.Test;
import test.schema.Pen;
import test.schema.Person;
import test.schema.PersonLegalDefence;
import test.schema.PersonSuit;

import java.sql.SQLException;

/**
 * @author Daniel Spiewak
 */
public class RelationsCacheTest extends DataTest {
	
	public RelationsCacheTest(int ordinal, TableNameConverter tableConverter, FieldNameConverter fieldConverter) throws SQLException {
		super(ordinal, tableConverter, fieldConverter);
	}

	@Test
	public void testOneToManyDestinationCreation() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.getPens();
		
		Pen pen = manager.create(Pen.class);

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
	public void testOneToManyDestinationDeletion() throws Exception
    {
		Pen pen = manager.create(Pen.class);
		final Person person = manager.get(Person.class, personID);
		person.getPens();

		manager.delete(pen);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });
    }

	@Test
	public void testOneToManyFieldModification() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		Pen pen = person.getPens()[0];
		
		pen.setDeleted(true);
		pen.save();

        Pen pen2 = sql.checkExecuted(new Command<Pen>()
        {
            public Pen run() throws Exception
            {
                return person.getPens()[0];
            }
        });

        pen2.setPerson(null);
        pen2.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPens();
                return null;
            }
        });

        pen2.setPerson(person);
		pen2.save();
		
		pen.setDeleted(false);
		pen.save();
	}
	
	@Test
	public void testManyToManyIntermediateCreation() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		person.getPersonLegalDefences();
		
		PersonSuit suit = manager.create(PersonSuit.class);

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        manager.delete(suit);
    }

    @Test
	public void testManyToManyIntermediateDeletion() throws Exception
    {
		PersonSuit suit = manager.create(PersonSuit.class);
		final Person person = manager.get(Person.class, personID);
		person.getPersonLegalDefences();
		
		manager.delete(suit);

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
	public void testManyToManyFieldModification() throws Exception
    {
		final Person person = manager.get(Person.class, personID);
		PersonLegalDefence defence = person.getPersonLegalDefences()[0];
		PersonSuit suit = manager.get(PersonSuit.class, suitIDs[0]);
		
		suit.setDeleted(true);
		suit.save();

        sql.checkExecuted(new Command<Void>()
        {
            public Void run() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        suit.setDeleted(false);
		suit.save();
		person.getPersonLegalDefences();
		
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

        suit.setPersonLegalDefence(defence);
		suit.save();
	}
}
