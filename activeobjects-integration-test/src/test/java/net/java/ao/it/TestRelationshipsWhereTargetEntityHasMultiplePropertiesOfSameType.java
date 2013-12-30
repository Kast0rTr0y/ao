package net.java.ao.it;

import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.ManyToMany;
import net.java.ao.OneToMany;
import net.java.ao.OneToOne;
import net.java.ao.Preload;
import net.java.ao.test.ActiveObjectsIntegrationTest;

/**
 * @see <a href="https://studio.atlassian.com/browse/AO-325">AO-325</a>
 */
public class TestRelationshipsWhereTargetEntityHasMultiplePropertiesOfSameType extends ActiveObjectsIntegrationTest
{

    public interface OneToOneNode extends Entity
    {

        @OneToOne(reverse = "getParent")
        OneToOneNode getChild();

        void setChild(OneToOneNode child);

        OneToOneNode getParent();

        void setParent(OneToOneNode parent);

        OneToOneNode getRelated();

        void setRelated(OneToOneNode related);

        String getName();

        void setName(String name);
    }

    /**
     * <p>Test an entity having a {@link OneToOne} relationship to an entity that has multiple properties of the same
     * type.</p>
     */
    @Test
    public void testOneToOne() throws Exception
    {
        entityManager.migrate(OneToOneNode.class);
        final OneToOneNode grandparent = entityManager.create(OneToOneNode.class, new DBParam("NAME", "grandparent"));
        final OneToOneNode parent = entityManager.create(OneToOneNode.class, new DBParam("NAME", "parent"), new DBParam("PARENT_ID", grandparent));
        final OneToOneNode child = entityManager.create(OneToOneNode.class, new DBParam("NAME", "child"), new DBParam("PARENT_ID", parent), new DBParam("RELATED_ID", grandparent));
        grandparent.setRelated(child);
        grandparent.save();
        Assert.assertNull(grandparent.getParent());
        Assert.assertEquals("grandparent", grandparent.getName());

        final OneToOneNode retrievedParent = grandparent.getChild();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(parent, retrievedParent);
                Assert.assertEquals("parent", retrievedParent.getName());
                return null;
            }
        });

        Assert.assertEquals(parent, grandparent.getChild());
        Assert.assertEquals(child, grandparent.getRelated());
        Assert.assertEquals(grandparent, parent.getParent());

        final OneToOneNode retrievedChild = parent.getChild();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(child, retrievedChild);
                Assert.assertEquals("child", retrievedChild.getName());
                return null;
            }
        });

        Assert.assertNull(parent.getRelated());
        Assert.assertEquals(parent, child.getParent());
        Assert.assertNull(child.getChild());
        Assert.assertEquals(grandparent, child.getRelated());
    }

    @Preload
    public interface PlOneToOneNode extends Entity
    {

        @OneToOne(reverse = "getParent")
        PlOneToOneNode getChild();

        void setChild(PlOneToOneNode child);

        PlOneToOneNode getParent();

        void setParent(PlOneToOneNode parent);

        PlOneToOneNode getRelated();

        void setRelated(PlOneToOneNode related);

        String getName();

        void setName(String name);
    }

    /**
     * <p>Test a @{link Preload}ed entity having a {@link OneToOne} relationship to an entity that has multiple
     * properties of the same type.</p>
     */
    @Test
    public void testOneToOneWithPreload() throws Exception
    {
        entityManager.migrate(PlOneToOneNode.class);
        final PlOneToOneNode grandparent = entityManager.create(PlOneToOneNode.class, new DBParam("NAME", "grandparent"));
        final PlOneToOneNode parent = entityManager.create(PlOneToOneNode.class, new DBParam("NAME", "parent"), new DBParam("PARENT_ID", grandparent));
        final PlOneToOneNode child = entityManager.create(PlOneToOneNode.class, new DBParam("NAME", "child"), new DBParam("PARENT_ID", parent), new DBParam("RELATED_ID", grandparent));
        grandparent.setRelated(child);
        grandparent.save();
        Assert.assertNull(grandparent.getParent());
        Assert.assertEquals("grandparent", grandparent.getName());

        final PlOneToOneNode retrievedParent = grandparent.getChild();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(parent, retrievedParent);
                Assert.assertEquals("parent", retrievedParent.getName());
                return null;
            }
        });

        Assert.assertEquals(child, grandparent.getRelated());
        Assert.assertEquals(grandparent, parent.getParent());

        final PlOneToOneNode retrievedChild = parent.getChild();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(child, retrievedChild);
                Assert.assertEquals("child", retrievedChild.getName());
                return null;
            }
        });

        Assert.assertNull(parent.getRelated());
        Assert.assertEquals(parent, child.getParent());
        Assert.assertNull(child.getChild());
        Assert.assertEquals(grandparent, child.getRelated());
    }

    public interface Adult extends Entity
    {

        @OneToMany(reverse = "getParent")
        Child[] getChildren();

        @OneToMany(reverse = "getTeacher")
        Child[] getStudents();

    }

    public interface Child extends Entity
    {

        Adult getParent();

        void setParent(Adult parent);

        Adult getTeacher();

        void setTeacher(Adult teacher);

        String getName();

        void setName(String name);
    }

    /**
     * <p>Test an entity having a {@link OneToMany} relationship to an entity that has multiple properties of the same
     * type.</p>
     */
    @Test
    public void testOneToMany() throws Exception
    {
        entityManager.migrate(Adult.class, Child.class);
        final Adult parent = entityManager.create(Adult.class);
        final Adult teacher = entityManager.create(Adult.class);
        final Child child = entityManager.create(Child.class, new DBParam("PARENT_ID", parent), new DBParam("TEACHER_ID", teacher), new DBParam("NAME", "Jim"));
        final Child[] children = {child};

        final Child[] retrievedChildren = parent.getChildren();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertArrayEquals(children, retrievedChildren);
                Assert.assertEquals("Jim", retrievedChildren[0].getName());
                return null;
            }
        });

        Assert.assertEquals(0, parent.getStudents().length);
        Assert.assertArrayEquals(children, teacher.getStudents());
        Assert.assertEquals(0, teacher.getChildren().length);
    }

    @Preload
    public interface PlAdult extends Entity
    {

        @OneToMany(reverse = "getParent")
        PlChild[] getChildren();

        @OneToMany(reverse = "getTeacher")
        PlChild[] getStudents();

    }

    @Preload
    public interface PlChild extends Entity
    {

        PlAdult getParent();

        void setParent(PlAdult parent);

        PlAdult getTeacher();

        void setTeacher(PlAdult teacher);

        String getName();

        void setName(String name);
    }

    /**
     * <p>Test a {@link Preload}ed entity having a {@link OneToMany} relationship to an entity that has multiple
     * properties of the same type.</p>
     */
    @Test
    public void testOneToManyWithPreload() throws Exception
    {
        entityManager.migrate(PlAdult.class, PlChild.class);
        final PlAdult parent = entityManager.create(PlAdult.class);
        final PlAdult teacher = entityManager.create(PlAdult.class);
        final PlChild child = entityManager.create(PlChild.class, new DBParam("PARENT_ID", parent), new DBParam("TEACHER_ID", teacher), new DBParam("NAME", "Jane"));
        final PlChild[] children = {child};

        final PlChild[] retrievedChildren = parent.getChildren();
        checkSqlNotExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertArrayEquals(children, retrievedChildren);
                Assert.assertEquals("Jane", retrievedChildren[0].getName());
                return null;
            }
        });

        Assert.assertEquals(0, parent.getStudents().length);
        Assert.assertArrayEquals(children, teacher.getStudents());
        Assert.assertEquals(0, teacher.getChildren().length);
    }

    public interface InvalidAdult extends Entity
    {

        @OneToMany(reverse = "parent")
        Child[] getChildren();

        @OneToMany(reverse = "teacher")
        Child[] getStudents();

    }

    public interface InvalidChild extends Entity
    {

        Adult getParent();

        void setParent(Adult parent);

        Adult getTeacher();

        void setTeacher(Adult teacher);

    }

    /**
     * <p>Test that an entity with an invalid {@link OneToMany#reverse()} value throws an exception during
     * {@link net.java.ao.EntityManager#migrate(Class[])}.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void testOneToManyWithInvalidReverse() throws Exception
    {
        entityManager.migrate(InvalidAdult.class, InvalidChild.class);
    }

    public interface ManyToManyNode extends Entity
    {

        @ManyToMany(value = ManyToManyEdge.class, reverse = "getOutput", through = "getInput")
        ManyToManyNode[] getInputs();

        @ManyToMany(value = ManyToManyEdge.class, reverse = "getInput", through = "getOutput")
        ManyToManyNode[] getOutputs();

        int getVal();

        void setVal(int val);
    }

    public interface ManyToManyEdge extends Entity
    {

        ManyToManyNode getInput();

        void setInput(ManyToManyNode input);

        ManyToManyNode getOutput();

        void setOutput(ManyToManyNode output);

    }

    /**
     * <p>Test an entity having a {@link net.java.ao.ManyToMany} relationship with a joining entity that has multiple
     * properties of the same type.</p>
     */
    @Test
    public void testManyToMany() throws Exception
    {
        entityManager.migrate(ManyToManyNode.class, ManyToManyEdge.class);
        final ManyToManyNode input = entityManager.create(ManyToManyNode.class, new DBParam("VAL", 1));
        final ManyToManyNode output = entityManager.create(ManyToManyNode.class, new DBParam("VAL", 2));
        entityManager.create(ManyToManyEdge.class, new DBParam("INPUT_ID", input), new DBParam("OUTPUT_ID", output));

        final ManyToManyNode[] inputOutputs = input.getOutputs();
        checkSqlNotExecuted(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                Assert.assertEquals(1, inputOutputs.length);
                Assert.assertEquals(2, inputOutputs[0].getVal());
                return null;
            }
        });
        Assert.assertArrayEquals(new ManyToManyNode[]{output}, inputOutputs);
        Assert.assertEquals(0, input.getInputs().length);

        final ManyToManyNode[] outputInputs = output.getInputs();
        checkSqlNotExecuted(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                Assert.assertEquals(1, outputInputs.length);
                Assert.assertEquals(1, outputInputs[0].getVal());
                return null;
            }
        });
        Assert.assertArrayEquals(new ManyToManyNode[]{input}, output.getInputs());
        Assert.assertEquals(0, output.getOutputs().length);
    }

    @Preload
    public interface PLManyToManyNode extends Entity
    {

        @ManyToMany(value = PLManyToManyEdge.class, reverse = "getOutput", through = "getInput")
        PLManyToManyNode[] getInputs();

        @ManyToMany(value = PLManyToManyEdge.class, reverse = "getInput", through = "getOutput")
        PLManyToManyNode[] getOutputs();

        int getVal();

        void setVal(int val);
    }

    public interface PLManyToManyEdge extends Entity
    {

        PLManyToManyNode getInput();

        void setInput(PLManyToManyNode input);

        PLManyToManyNode getOutput();

        void setOutput(PLManyToManyNode output);

    }

    /**
     * <p>Test an entity having a {@link net.java.ao.ManyToMany} relationship with a joining entity that has multiple
     * properties of the same type.</p>
     */
    @Test
    public void testManyToManyWithPreload() throws Exception
    {
        entityManager.migrate(PLManyToManyNode.class, PLManyToManyEdge.class);
        final PLManyToManyNode input = entityManager.create(PLManyToManyNode.class, new DBParam("VAL", 1));
        final PLManyToManyNode output = entityManager.create(PLManyToManyNode.class, new DBParam("VAL", 2));
        entityManager.create(PLManyToManyEdge.class, new DBParam("INPUT_ID", input), new DBParam("OUTPUT_ID", output));

        final PLManyToManyNode[] inputOutputs = input.getOutputs();
        checkSqlNotExecuted(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                Assert.assertEquals(1, inputOutputs.length);
                Assert.assertEquals(2, inputOutputs[0].getVal());
                Assert.assertArrayEquals(new PLManyToManyNode[] { output }, inputOutputs);
                return null;
            }
        });
        checkSqlExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(0, input.getInputs().length);
                return null;
            }
        });

        final PLManyToManyNode[] outputInputs = output.getInputs();
        checkSqlNotExecuted(new Callable<Object>()
        {
            public Object call() throws Exception
            {
                Assert.assertEquals(1, outputInputs.length);
                Assert.assertEquals(1, outputInputs[0].getVal());
                Assert.assertArrayEquals(new PLManyToManyNode[] { input }, outputInputs);
                return null;
            }
        });
        checkSqlExecuted(new Callable<Object>()
        {
            @Override
            public Object call() throws Exception
            {
                Assert.assertEquals(0, output.getOutputs().length);
                return null;
            }
        });
    }

}
