package net.java.ao.builder;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 *
 * @see EntityManagerBuilder
 * @see EntityManagerBuilderWithUrlAndUsername
 * @see EntityManagerBuilderWithDatabaseProvider
 */
public final class EntityManagerBuilderWithUrl
{
    private final String url;

    EntityManagerBuilderWithUrl(String url)
    {
        this.url = url;
    }

    public EntityManagerBuilderWithUrlAndUsername username(String username)
    {
        return new EntityManagerBuilderWithUrlAndUsername(url, username);
    }
}
