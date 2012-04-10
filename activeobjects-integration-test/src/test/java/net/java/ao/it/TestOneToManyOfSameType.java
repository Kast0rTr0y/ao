package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.OneToMany;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

public class TestOneToManyOfSameType extends ActiveObjectsIntegrationTest
{

    public interface Adult extends Entity
    {

        @OneToMany(/*target = "parent"*/)
        Child[] getChildren();

        @OneToMany(/*target = "teacher"*/)
        Child[] getStudents();

    }

    public interface Child extends Entity
    {

        Adult getParent();

        void setParent(Adult parent);

        Adult getTeacher();

        void setTeacher(Adult teacher);

    }

    /**
     * <p>Test an entity having more than one {@link OneToMany} relationship of the same type.</p>
     *
     * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
     */
    @Test
    public void testOneToManyOfSameType() throws Exception
    {
        entityManager.migrate(Adult.class, Child.class);
        final Adult parent = entityManager.create(Adult.class);
        final Adult teacher = entityManager.create(Adult.class);
        final Child child = entityManager.create(Child.class, new DBParam("parent_id", parent), new DBParam("teacher_id", teacher));
        final Child[] children = {child};
        Assert.assertArrayEquals(children, parent.getChildren());
        Assert.assertEquals(0, parent.getStudents().length);
        Assert.assertArrayEquals(children, teacher.getStudents());
        Assert.assertEquals(0, teacher.getChildren().length);
    }

}
