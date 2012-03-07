package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.OneToOne;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

@Data(TestMultipleOneToOneOnOneEntity.DataUpdater.class)
public final class TestMultipleOneToOneOnOneEntity extends ActiveObjectsIntegrationTest
{
    @Test
    public void test() throws Exception
    {
        final X x = entityManager.create(X.class);
        assertTrue(x.getID() > 0);

        final WindowTypeXref square = createWindowTypeXref(x, "square");
        assertTrue(square.getID() > 0);
        assertEquals(x.getID(), square.getX().getID());

        final WindowTypeXref round = createWindowTypeXref(x, "round");
        assertTrue(round.getID() > 0);
        assertEquals(x.getID(), round.getX().getID());

        final WindowTypeXref oval = createWindowTypeXref(x, "oval");
        assertTrue(oval.getID() > 0);
        assertEquals(x.getID(), oval.getX().getID());


        assertEquals(square.getID(), x.getTheSquareWindow().getID());
        assertEquals(round.getID(), x.getTheRoundWindow().getID());
        assertEquals(oval.getID(), x.getTheOvalWindow().getID());
    }

    private WindowTypeXref createWindowTypeXref(X x, String type) throws SQLException
    {
        final WindowTypeXref w = entityManager.create(WindowTypeXref.class);
        w.setX(x);
        w.setWindowType(type);
        w.save();
        
        return w;
    }

    interface X extends Entity
    {
        @OneToOne(where = "WINDOW_TYPE='square'")
        WindowTypeXref getTheSquareWindow();

        @OneToOne(where = "WINDOW_TYPE='round'")
        WindowTypeXref getTheRoundWindow();

        @OneToOne(where = "WINDOW_TYPE='oval'")
        WindowTypeXref getTheOvalWindow();
    }

    interface WindowTypeXref extends Entity
    {
        public X getX();

        public void setX(X anX);

        public void setWindowType(String t);

        public String getWindowType();
    }

    public static class DataUpdater implements DatabaseUpdater
    {
        @Override
        public void update(EntityManager entityManager) throws Exception
        {
            entityManager.migrate(X.class, WindowTypeXref.class);
        }
    }
}

