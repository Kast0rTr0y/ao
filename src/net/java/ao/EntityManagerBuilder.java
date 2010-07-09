package net.java.ao;

/**
 * <p>Class to make it easier to build a fully configured {@link net.java.ao.EntityManager}</p>
 * <p>This is the root class of a set that helps developers build a correctly configured manager</p>
 * @see EntityManagerBuilderWithUrl
 * @see net.java.ao.EntityManagerBuilderWithUrlAndUsername
 * @see net.java.ao.EntityManagerBuilderWithDatabaseProvider
 */
public class EntityManagerBuilder
{
    public static EntityManagerBuilderWithUrl url(String url)
    {
        return new EntityManagerBuilderWithUrl(url);
    }
}
