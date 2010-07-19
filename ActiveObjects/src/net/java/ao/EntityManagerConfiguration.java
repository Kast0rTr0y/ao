package net.java.ao;

/**
 * This represents a configuration for entity manager creation.
 */
public interface EntityManagerConfiguration
{
    /**
     * Whether or not the {@link net.java.ao.EntityManager} should use weak references for caching
     * @return {@code true} if the entity manager should use weak references for caching.
     */
    boolean useWeakCache();
}
