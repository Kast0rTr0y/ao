package net.java.ao.builder;

import net.java.ao.EntityManagerConfiguration;
import net.java.ao.DatabaseProvider;
import net.java.ao.LuceneConfiguration;
import net.java.ao.SearchableEntityManager;
import net.java.ao.event.EventManager;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class EntityManagerBuilderWithDatabaseProviderAndLuceneConfiguration
{
    private final DatabaseProvider databaseProvider;
    private final EntityManagerConfiguration configuration;
    private final EventManager eventManager;
    private final LuceneConfiguration luceneConfiguration;

    public EntityManagerBuilderWithDatabaseProviderAndLuceneConfiguration(DatabaseProvider databaseProvider, EntityManagerConfiguration configuration, EventManager eventManager, LuceneConfiguration luceneConfiguration)
    {
        this.databaseProvider = checkNotNull(databaseProvider);
        this.luceneConfiguration = checkNotNull(luceneConfiguration);
        this.configuration = checkNotNull(configuration);
        this.eventManager = checkNotNull(eventManager);
    }

    public SearchableEntityManager build()
    {
        try
        {
            return new SearchableEntityManager(databaseProvider, configuration, eventManager, luceneConfiguration);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
