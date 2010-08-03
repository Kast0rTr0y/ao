package net.java.ao.builder;

import net.java.ao.LuceneConfiguration;
import net.java.ao.SearchableEntityManager;
import net.java.ao.event.EventManager;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.builder.DatabaseProviderFactory.getDatabaseProvider;

public final class EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration extends AbstractEntityManagerBuilderWithDatabaseProperties<EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration>
{
    private final LuceneConfiguration luceneConfiguration;

    EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration(DatabaseProperties databaseProperties, BuilderEntityManagerConfiguration configuration, EventManager eventManager, LuceneConfiguration luceneConfiguration)
    {
        super(databaseProperties, configuration, eventManager);
        this.luceneConfiguration = checkNotNull(luceneConfiguration);
    }

    public SearchableEntityManager build()
    {
        try
        {
            return new SearchableEntityManager(getDatabaseProvider(getDatabaseProperties()), getEntityManagerConfiguration(), getEventManager(), luceneConfiguration);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
