package net.java.ao.it;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.java.ao.EntityStreamCallback;
import net.java.ao.it.DatabaseProcessor.PersonData;
import net.java.ao.it.model.OnlineDistribution;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;

import org.junit.Test;

@Data(DatabaseProcessor.class)
public class ReadOnlyEntityIntegrationTest extends ActiveObjectsIntegrationTest
{

    /**
     * Test that calling a getter on the interface is properly supported by the proxy
     */
    @Test
    public void testGetter()
    {
        Person person = getPerson();

        assertEquals(PersonData.FIRST_NAME, person.getFirstName());
    }

    /**
     * Test that using an accessor annotation works as well
     */
    @Test
    public void testNonGetterAccessor()
    {
        OnlineDistribution dist = getOnlineDistribution();
        // reference is hidden, so we can't easily compare. Not null will suffice.
        assertNotNull(dist.getURL());
    }

    /**
     * Test that we can use a getter declared in a superinterface
     */
    @Test
    public void testSuperInterfaceGetter()
    {
        OnlineDistribution dist = getOnlineDistribution();

        assertNotNull(dist.getID());
    }

    /**
     * Test that calling a getter for a related object (like a one-to-many getter) returns null and doesn't throw an exception null because these
     * don't make sense for lazy loading. If object graph materialisation through JOINS is supported, this can be removed.
     */
    @Test
    public void testRelationGetter()
    {
        Person person = getPerson();

        assertNull(person.getNose());
    }

    /**
     * Test that calling a setter causes a runtime exception, because that would leave the client in a dangerous assumption state otherwise
     */
    @Test(expected = RuntimeException.class)
    public void testSetter()
    {
        getPerson().setAge(99);
    }

    /**
     * Test that calling save throws a runtime exception. This isn't supported on read-only views, and would leave the client in a dangerous
     * assumption state if there's no exception thrown.
     */
    @Test(expected = RuntimeException.class)
    public void testSave()
    {
        getPerson().save();
    }

    /**
     * Test that calling other, non-critical mutating methods on the proxy don't cause exceptions
     */
    @Test
    public void testNoopProxyMethods()
    {
        PropertyChangeListener listener = new PropertyChangeListener()
        {
            @Override
            public void propertyChange(PropertyChangeEvent evt)
            {
                fail();
            }
        };
        
        Person person = getPerson();
        person.addPropertyChangeListener(listener);
        person.removePropertyChangeListener(listener);
    }

    @Test
    public void testToString()
    {
        Person person = getPerson();
        
        assertNotNull(person.toString());
    }

    @Test
    public void testHashCode()
    {
        Person person = getPerson();
        
        assertNotNull(person.hashCode());
    }

    @Test
    public void testEquals()
    {
        Person person0 = getPerson();
        Person person1 = getPerson();
        
        assertTrue(person0.equals(person1));
    }

    private Person getPerson()
    {
        final List<Person> persons = new ArrayList<Person>();

        try
        {
            entityManager.stream(Person.class, new EntityStreamCallback<Person, Integer>()
            {
                @Override
                public void onRowRead(Person t)
                {
                    persons.add(t);
                }
            });
        } catch (SQLException e)
        {
            throw new RuntimeException(e);
        }

        if (persons.size() == 0) fail("There should have been at least one person read from the dataset");
        return persons.get(0);
    }

    private OnlineDistribution getOnlineDistribution()
    {
        final List<OnlineDistribution> distributions = new ArrayList<OnlineDistribution>();

        try
        {
            entityManager.stream(OnlineDistribution.class, new EntityStreamCallback<OnlineDistribution, Integer>()
            {
                @Override
                public void onRowRead(OnlineDistribution t)
                {
                    distributions.add(t);
                }
            });
        } catch (SQLException e)
        {
            throw new RuntimeException(e);
        }

        if (distributions.size() == 0) fail("There should have been at least one OnlineDistribution read from the dataset");
        return distributions.get(0);
    }

}
