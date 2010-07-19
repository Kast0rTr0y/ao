package net.java.ao.builder;

import net.java.ao.EntityManagerConfiguration;
import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.LuceneConfiguration;
import net.java.ao.event.EventManager;
import net.java.ao.event.EventManagerImpl;
import org.apache.lucene.store.Directory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 *
 * @see EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see EntityManagerBuilderWithUrlAndUsername
 */
public final class EntityManagerBuilderWithDatabaseProvider
{
    private final DatabaseProvider databaseProvider;
    private final BuilderEntityManagerConfiguration configuration;
    private final EventManager eventManager;

    EntityManagerBuilderWithDatabaseProvider(DatabaseProvider databaseProvider)
    {
        this.databaseProvider = checkNotNull(databaseProvider);
        this.configuration = new BuilderEntityManagerConfiguration();
        this.eventManager = new EventManagerImpl();
    }

    public EntityManagerBuilderWithDatabaseProvider useWeakCache()
    {
        configuration.setUseWeakCache(true);
        return this;
    }

    public EntityManagerBuilderWithDatabaseProviderAndLuceneConfiguration withIndex(final Directory indexDir)
    {
        checkNotNull(indexDir);
        return new EntityManagerBuilderWithDatabaseProviderAndLuceneConfiguration(databaseProvider, configuration, eventManager, new LuceneConfiguration()
        {
            public Directory getIndexDirectory()
            {
                return indexDir;
            }
        });
    }

    public EntityManager build()
    {
        return new EntityManager(databaseProvider, configuration, new EventManagerImpl());
    }

    private static class BuilderEntityManagerConfiguration implements EntityManagerConfiguration
    {
        private boolean useWeakCache = false;

        public boolean useWeakCache()
        {
            return useWeakCache;
        }

        public void setUseWeakCache(boolean useWeakCache)
        {
            this.useWeakCache = useWeakCache;
        }
    }
}
