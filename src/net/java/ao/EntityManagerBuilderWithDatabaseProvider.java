package net.java.ao;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 * @see net.java.ao.EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see net.java.ao.EntityManagerBuilderWithUrlAndUsername
 */
public class EntityManagerBuilderWithDatabaseProvider
{
    private final DatabaseProvider databaseProvider;

    EntityManagerBuilderWithDatabaseProvider(DatabaseProvider databaseProvider)
    {
        this.databaseProvider = databaseProvider;
    }

    public EntityManager build()
    {
        return new EntityManager(databaseProvider);
    }
}
