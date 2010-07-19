package net.java.ao.builder;

import net.java.ao.EntityManagerConfiguration;
import net.java.ao.EntityManager;
import net.java.ao.LuceneConfiguration;
import net.java.ao.event.EventManager;
import net.java.ao.event.EventManagerImpl;
import org.apache.lucene.store.Directory;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.builder.DatabaseProviderFactory.getDatabaseProvider;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 *
 * @see EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see EntityManagerBuilderWithUrlAndUsername
 */
public final class EntityManagerBuilderWithDatabaseProperties
{
    private final DatabaseProperties databaseProperties;
    private final BuilderEntityManagerConfiguration configuration;
    private final EventManager eventManager;

    EntityManagerBuilderWithDatabaseProperties(DatabaseProperties databaseProperties)
    {
        this.databaseProperties = checkNotNull(databaseProperties);
        this.configuration = new BuilderEntityManagerConfiguration();
        this.eventManager = new EventManagerImpl();
    }

    public EntityManagerBuilderWithDatabaseProperties useWeakCache()
    {
        configuration.setUseWeakCache(true);
        return this;
    }

    public EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration withIndex(final Directory indexDir)
    {
        checkNotNull(indexDir);
        return new EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration(databaseProperties, configuration, eventManager, new LuceneConfiguration()
        {
            public Directory getIndexDirectory()
            {
                return indexDir;
            }
        });
    }

    public EntityManager build()
    {
        return new EntityManager(getDatabaseProvider(databaseProperties), configuration, new EventManagerImpl());
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
