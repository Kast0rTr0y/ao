package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.ManyToMany;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

public class TestManyToManyOfSameType extends ActiveObjectsIntegrationTest
{

    public interface Person extends Entity
    {

        @ManyToMany(Parenthood.class)
        Person[] getChildren();

        @ManyToMany(Parenthood.class)
        Person[] getParents();

    }

    public interface Parenthood extends Entity
    {

        Person getParent();

        void setParent(Person parent);

        Person getChild();

        void setChild(Person child);

    }

    /**
     * <p>Test an entity having more than one {@link net.java.ao.ManyToMany} relationship of the same type.</p>
     *
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Test
    public void testManyToManyOfSameType() throws Exception
    {
        entityManager.migrate(Person.class, Parenthood.class);
        final Person parent = entityManager.create(Person.class);
        final Person child = entityManager.create(Person.class);
        entityManager.create(Parenthood.class, new DBParam("parent_id", parent), new DBParam("child_id", child));
        Assert.assertArrayEquals(new Person[]{child}, parent.getChildren());
        Assert.assertEquals(0, parent.getParents().length);
        Assert.assertArrayEquals(new Person[]{parent}, child.getParents());
        Assert.assertEquals(0, child.getChildren().length);
    }

}
