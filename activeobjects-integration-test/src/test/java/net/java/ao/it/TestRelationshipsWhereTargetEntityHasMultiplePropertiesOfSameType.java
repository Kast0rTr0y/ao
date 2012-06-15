package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.ManyToMany;
import net.java.ao.OneToMany;
import net.java.ao.OneToOne;
import net.java.ao.Preload;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import org.junit.Assert;
import org.junit.Test;

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

    }

    /**
     * <p>Test an entity having a {@link OneToOne} relationship to an entity that has multiple properties of the same
     * type.</p>
     */
    @Test
    public void testOneToOne() throws Exception
    {
        entityManager.migrate(OneToOneNode.class);
        final OneToOneNode grandparent = entityManager.create(OneToOneNode.class);
        final OneToOneNode parent = entityManager.create(OneToOneNode.class, new DBParam("PARENT_ID", grandparent));
        final OneToOneNode child = entityManager.create(OneToOneNode.class, new DBParam("PARENT_ID", parent), new DBParam("RELATED_ID", grandparent));
        grandparent.setRelated(child);
        grandparent.save();
        Assert.assertNull(grandparent.getParent());
        Assert.assertSame(parent, grandparent.getChild());
        Assert.assertSame(child, grandparent.getRelated());
        Assert.assertSame(grandparent, parent.getParent());
        Assert.assertSame(child, parent.getChild());
        Assert.assertNull(parent.getRelated());
        Assert.assertSame(parent, child.getParent());
        Assert.assertNull(child.getChild());
        Assert.assertSame(grandparent, child.getRelated());
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
        final Child child = entityManager.create(Child.class, new DBParam("PARENT_ID", parent), new DBParam("TEACHER_ID", teacher));
        final Child[] children = {child};
        Assert.assertArrayEquals(children, parent.getChildren());
        Assert.assertEquals(0, parent.getStudents().length);
        Assert.assertArrayEquals(children, teacher.getStudents());
        Assert.assertEquals(0, teacher.getChildren().length);
    }

    @Preload
    public interface PreloadAdult extends Entity
    {

        @OneToMany(reverse = "getParent")
        PreloadChild[] getChildren();

        @OneToMany(reverse = "getTeacher")
        PreloadChild[] getStudents();

    }

    @Preload
    public interface PreloadChild extends Entity
    {

        PreloadAdult getParent();

        void setParent(PreloadAdult parent);

        PreloadAdult getTeacher();

        void setTeacher(PreloadAdult teacher);

    }

    /**
     * <p>Test a {@link Preload}ed entity having a {@link OneToMany} relationship to an entity that has multiple
     * properties of the same type.</p>
     */
    @Test
    public void testOneToManyWithPreload() throws Exception
    {
        entityManager.migrate(PreloadAdult.class, PreloadChild.class);
        final PreloadAdult parent = entityManager.create(PreloadAdult.class);
        final PreloadAdult teacher = entityManager.create(PreloadAdult.class);
        final PreloadChild child = entityManager.create(PreloadChild.class, new DBParam("PARENT_ID", parent), new DBParam("TEACHER_ID", teacher));
        final PreloadChild[] children = {child};
        Assert.assertArrayEquals(children, parent.getChildren());
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

        @ManyToMany(value = ManyToManyEdge.class, reverse = "getOutputs", through = "getInput")
        ManyToManyNode[] getInputs();

        @ManyToMany(value = ManyToManyEdge.class, reverse = "getInputs", through = "getOutput")
        ManyToManyNode[] getOutputs();

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
        final ManyToManyNode input = entityManager.create(ManyToManyNode.class);
        final ManyToManyNode output = entityManager.create(ManyToManyNode.class);
        entityManager.create(ManyToManyEdge.class, new DBParam("INPUT_ID", input), new DBParam("OUTPUT_ID", output));
        Assert.assertArrayEquals(new ManyToManyNode[]{output}, input.getOutputs());
        Assert.assertEquals(0, input.getInputs().length);
        Assert.assertArrayEquals(new ManyToManyNode[]{input}, output.getInputs());
        Assert.assertEquals(0, output.getOutputs().length);
    }

}
