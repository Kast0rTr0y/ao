package net.java.ao.it;

import net.java.ao.EntityManager;
import net.java.ao.test.jdbc.DatabaseUpdater;

/**
 *
 */
public final class DatabaseProcessor implements DatabaseUpdater
{
    public void update(EntityManager entityManager)
    {
        try
        {
            Database.get(entityManager);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
