package net.java.ao;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 * @see net.java.ao.EntityManagerBuilder
 * @see net.java.ao.EntityManagerBuilderWithUrlAndUsername
 * @see net.java.ao.EntityManagerBuilderWithDatabaseProvider
 */
public class EntityManagerBuilderWithUrl
{
    private final String url;

    EntityManagerBuilderWithUrl(String url)
    {
        this.url = url;
    }

    EntityManagerBuilderWithUrlAndUsername username(String username)
    {
        return new EntityManagerBuilderWithUrlAndUsername(url, username);
    }
}
