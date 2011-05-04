package net.java.ao.builder;

import net.java.ao.LuceneConfiguration;
import net.java.ao.SearchableEntityManager;

import java.io.IOException;

import static com.google.common.base.Preconditions.*;
import static net.java.ao.builder.DatabaseProviderFactory.*;

public final class EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration extends AbstractEntityManagerBuilderWithDatabaseProperties<EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration>
{
    private final LuceneConfiguration luceneConfiguration;

    EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration(DatabaseProperties databaseProperties, BuilderEntityManagerConfiguration configuration, LuceneConfiguration luceneConfiguration)
    {
        super(databaseProperties, configuration);
        this.luceneConfiguration = checkNotNull(luceneConfiguration);
    }

    public SearchableEntityManager build()
    {
        try
        {
            return new SearchableEntityManager(getDatabaseProvider(getDatabaseProperties()), getEntityManagerConfiguration(), luceneConfiguration);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
