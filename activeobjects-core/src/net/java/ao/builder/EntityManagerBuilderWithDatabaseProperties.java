package net.java.ao.builder;

import net.java.ao.EntityManager;
import net.java.ao.LuceneConfiguration;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.builder.DatabaseProviderFactory.getDatabaseProvider;

/**
 * This is class used to build {@link net.java.ao.EntityManager}
 *
 * @see EntityManagerBuilder
 * @see EntityManagerBuilderWithUrl
 * @see EntityManagerBuilderWithUrlAndUsername
 */
public final class EntityManagerBuilderWithDatabaseProperties extends AbstractEntityManagerBuilderWithDatabaseProperties<EntityManagerBuilderWithDatabaseProperties>
{
    EntityManagerBuilderWithDatabaseProperties(DatabaseProperties databaseProperties)
    {
        super(databaseProperties);
    }

    public EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration withIndex(final File indexDir)
    {
        checkNotNull(indexDir);
        return new EntityManagerBuilderWithDatabasePropertiesAndLuceneConfiguration(getDatabaseProperties(), getEntityManagerConfiguration(), getEventManager(), new LuceneConfiguration()
        {
            public Directory getIndexDirectory()
            {
                try
                {
                    return FSDirectory.getDirectory(indexDir);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public EntityManager build()
    {
        return new EntityManager(getDatabaseProvider(getDatabaseProperties()), getEntityManagerConfiguration(), getEventManager());
    }
}
