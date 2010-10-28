package net.java.ao.test.jdbc;

import net.java.ao.EntityManager;

/**
 *
 */
public interface DatabaseUpdater
{
    void update(EntityManager entityManager) throws Exception;
}
