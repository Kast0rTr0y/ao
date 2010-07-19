package net.java.ao.builder;

import net.java.ao.DatabaseProvider;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 *
 * @see EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see EntityManagerBuilderWithDatabaseProvider
 */
public final class EntityManagerBuilderWithUrlAndUsername
{
    private final String url;
    private final String username;

    EntityManagerBuilderWithUrlAndUsername(String url, String username)
    {
        this.url = url;
        this.username = username;
    }

    public EntityManagerBuilderWithDatabaseProvider password(String password)
    {
        return new EntityManagerBuilderWithDatabaseProvider(DatabaseProvider.getInstance(url, username, password));
    }
}
