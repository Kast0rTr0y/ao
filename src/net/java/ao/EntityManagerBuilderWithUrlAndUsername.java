package net.java.ao;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 * @see net.java.ao.EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see net.java.ao.EntityManagerBuilderWithDatabaseProvider
 */
public class EntityManagerBuilderWithUrlAndUsername
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
