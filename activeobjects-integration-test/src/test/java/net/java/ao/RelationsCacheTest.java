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
import net.java.ao.test.jdbc.DynamicJdbcConfiguration;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonLegalDefence;
import net.java.ao.it.model.PersonSuit;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import org.junit.Test;

import java.util.concurrent.Callable;

import static net.java.ao.it.DatabaseProcessor.*;

/**
 * @author Daniel Spiewak
 */
@Data(DatabaseProcessor.class)
@Jdbc(DynamicJdbcConfiguration.class)
public class RelationsCacheTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testOneToManyDestinationCreation() throws Exception
    {
        final Person person = getPerson();
        person.getPens();

        Pen pen = entityManager.create(Pen.class);

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                person.getPens();
                return null;
            }
        });

        entityManager.delete(pen);
    }

    @Test
    public void testOneToManyDestinationDeletion() throws Exception
    {
        Pen pen = entityManager.create(Pen.class);
        final Person person = getPerson();
        person.getPens();

        entityManager.delete(pen);

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                person.getPens();
                return null;
            }
        });
    }

    @Test
    public void testOneToManyFieldModification() throws Exception
    {
        final Person person = getPerson();
        Pen pen = person.getPens()[0];

        pen.setDeleted(true);
        pen.save();

        Pen pen2 = checkSqlExecuted(new Callable<Pen>()
        {
            public Pen call() throws Exception
            {
                return person.getPens()[0];
            }
        });

        pen2.setPerson(null);
        pen2.save();

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
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
        final Person person = getPerson();
        person.getPersonLegalDefences();

        PersonSuit suit = entityManager.create(PersonSuit.class);

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        entityManager.delete(suit);
    }

    @Test
    public void testManyToManyIntermediateDeletion() throws Exception
    {
        PersonSuit suit = entityManager.create(PersonSuit.class);
        final Person person = getPerson();
        person.getPersonLegalDefences();

        entityManager.delete(suit);

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });
    }

    @Test
    public void testManyToManyFieldModification() throws Exception
    {
        final Person person = getPerson();
        PersonLegalDefence defence = person.getPersonLegalDefences()[0];
        PersonSuit suit = entityManager.get(PersonSuit.class, PersonSuitData.getIds()[0]);
        suit.setDeleted(true);
        suit.save();

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
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

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
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

        checkSqlExecuted(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                person.getPersonLegalDefences();
                return null;
            }
        });

        suit.setPersonLegalDefence(defence);
        suit.save();
    }

    private Person getPerson()
    {
        return entityManager.get(Person.class, PersonData.getId());
    }
}
