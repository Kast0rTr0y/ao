package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.ManyToMany;
import net.java.ao.Polymorphic;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

public class TestPolymorphicManyToMany extends ActiveObjectsIntegrationTest {

    @Polymorphic
    public interface Person extends Entity {

        String getName();

        void setName(String name);

        @ManyToMany(value = Parenthood.class, reverse = "getParent", through = "getChild")
        Person[] getChildren();

        @ManyToMany(value = Parenthood.class, reverse = "getChild", through = "getParent")
        Person[] getParents();

    }

    public interface Man extends Person {

    }

    public interface Woman extends Person {

    }

    public interface Parenthood extends Entity {

        Person getParent();

        void setParent(Person parent);

        Person getChild();

        void setChild(Person child);

    }

    /**
     * <p>Test {@link net.java.ao.ManyToMany} relationships involving entities of the same {@link net.java.ao.Polymorphic}
     * type.</p>
     *
     * @see <a href="AO-148">https://studio.atlassian.com/browse/AO-148</a>
     */
    @Test
    public void testPolymorphicManyToMany() throws Exception {
        entityManager.migrate(Man.class, Woman.class, Parenthood.class);
        final Person parent = entityManager.create(Man.class, new DBParam("NAME", "John Smith"));
        final Person child = entityManager.create(Woman.class, new DBParam("NAME", "Sarah Smith"));
        entityManager.create(Parenthood.class, new DBParam("PARENT_ID", parent), new DBParam("PARENT_TYPE", Man.class.getName()), new DBParam("CHILD_ID", child), new DBParam("CHILD_TYPE", Woman.class.getName()));
        Assert.assertArrayEquals(new Person[]{child}, parent.getChildren());
        Assert.assertEquals(0, parent.getParents().length);
    }

}
