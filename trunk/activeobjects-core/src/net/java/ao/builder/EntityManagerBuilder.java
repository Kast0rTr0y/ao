package net.java.ao.builder;

/**
 * <p>Class to make it easier to build a fully configured {@link net.java.ao.EntityManager}</p>
 * <p>This is the root class of a set that helps developers build a correctly configured manager</p>
 *
 * @see EntityManagerBuilderWithUrl
 * @see EntityManagerBuilderWithUrlAndUsername
 * @see EntityManagerBuilderWithDatabaseProperties
 */
public final class EntityManagerBuilder
{
    public static EntityManagerBuilderWithUrl url(String url)
    {
        return new EntityManagerBuilderWithUrl(url);
    }
}
