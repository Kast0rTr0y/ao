package net.java.ao.test.jdbc;

import net.java.ao.EntityManager;

public final class NullDatabase implements DatabaseUpdater {
    private NullDatabase() {
    }

    @Override
    public void update(EntityManager entityManager) throws Exception {
    }
}
