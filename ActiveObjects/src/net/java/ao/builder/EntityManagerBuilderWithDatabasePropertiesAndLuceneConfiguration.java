package net.java.ao.builder;

import net.java.ao.EntityManagerConfiguration;
import net.java.ao.LuceneConfiguration;
import net.java.ao.SearchableEntityManager;
import net.java.ao.event.EventManager;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.builder.DatabaseProviderFactory.getDatabaseProvider;

public final class EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration
{
    private final DatabaseProperties databaseProperties;
    private final EntityManagerConfiguration configuration;
    private final EventManager eventManager;
    private final LuceneConfiguration luceneConfiguration;

    public EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration(DatabaseProperties databaseProperties, EntityManagerConfiguration configuration, EventManager eventManager, LuceneConfiguration luceneConfiguration)
    {
        this.databaseProperties = checkNotNull(databaseProperties);
        this.luceneConfiguration = checkNotNull(luceneConfiguration);
        this.configuration = checkNotNull(configuration);
        this.eventManager = checkNotNull(eventManager);
    }

    public SearchableEntityManager build()
    {
        try
        {
            return new SearchableEntityManager(getDatabaseProvider(databaseProperties), configuration, eventManager, luceneConfiguration);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
