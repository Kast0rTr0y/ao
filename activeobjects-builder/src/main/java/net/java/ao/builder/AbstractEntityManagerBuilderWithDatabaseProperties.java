package net.java.ao.builder;

import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.EntityManager;
import net.java.ao.EntityManagerConfiguration;
import net.java.ao.SchemaConfiguration;
import net.java.ao.event.EventManager;
import net.java.ao.event.EventManagerImpl;
import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableAnnotationTableNameConverter;
import net.java.ao.schema.TableNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractEntityManagerBuilderWithDatabaseProperties<B extends AbstractEntityManagerBuilderWithDatabaseProperties>
{
    private final DatabaseProperties databaseProperties;
    private final BuilderEntityManagerConfiguration configuration;

    private EventManager eventManager;

    AbstractEntityManagerBuilderWithDatabaseProperties(DatabaseProperties databaseProperties)
    {
        this(databaseProperties, new BuilderEntityManagerConfiguration(), null);
    }

    AbstractEntityManagerBuilderWithDatabaseProperties(DatabaseProperties databaseProperties, BuilderEntityManagerConfiguration configuration, EventManager eventManager)
    {
        this.databaseProperties = checkNotNull(databaseProperties);
        this.configuration = checkNotNull(configuration);
        this.eventManager = eventManager; // can be null
    }

    public B tableNameConverter(TableNameConverter tableNameConverter)
    {
        configuration.setTableNameConverter(checkNotNull(tableNameConverter));
        return cast();
    }

    public B fieldNameConverter(FieldNameConverter fieldNameConverter)
    {
        configuration.setFieldNameConverter(checkNotNull(fieldNameConverter));
        return cast();
    }

    public B schemaConfiguration(SchemaConfiguration schemaConfiguration)
    {
        configuration.setSchemaConfiguration(schemaConfiguration);
        return cast();
    }

    public B useWeakCache()
    {
        configuration.setUseWeakCache(true);
        return cast();
    }

    public B eventManager(EventManager eventManager)
    {
        this.eventManager = checkNotNull(eventManager);
        return cast();
    }

    final EventManager getEventManager()
    {
        return eventManager != null ? eventManager : new EventManagerImpl();
    }

    final DatabaseProperties getDatabaseProperties()
    {
        return databaseProperties;
    }

    final BuilderEntityManagerConfiguration getEntityManagerConfiguration()
    {
        return configuration;
    }

    public abstract EntityManager build();

    @SuppressWarnings("unchecked")
    private B cast()
    {
        return (B) this;
    }

    static class BuilderEntityManagerConfiguration implements EntityManagerConfiguration
    {
        private SchemaConfiguration schemaConfiguration;
        private TableNameConverter tableNameConverter;
        private FieldNameConverter fieldNameConverter;
        private boolean useWeakCache = false;

        public boolean useWeakCache()
        {
            return useWeakCache;
        }

        public TableNameConverter getTableNameConverter()
        {
            return tableNameConverter != null ? tableNameConverter : defaultTableNameConverter();
        }

        private static TableNameConverter defaultTableNameConverter()
        {
            return new TableAnnotationTableNameConverter(new CamelCaseTableNameConverter());
        }

        public FieldNameConverter getFieldNameConverter()
        {
            return fieldNameConverter != null ? fieldNameConverter : defaultFieldNameConverter();
        }

        private static CamelCaseFieldNameConverter defaultFieldNameConverter()
        {
            return new CamelCaseFieldNameConverter();
        }

        public void setUseWeakCache(boolean useWeakCache)
        {
            this.useWeakCache = useWeakCache;
        }

        public void setTableNameConverter(TableNameConverter tableNameConverter)
        {
            this.tableNameConverter = tableNameConverter;
        }

        public void setFieldNameConverter(FieldNameConverter fieldNameConverter)
        {
            this.fieldNameConverter = fieldNameConverter;
        }

        public SchemaConfiguration getSchemaConfiguration()
        {
            return schemaConfiguration != null ? schemaConfiguration : new DefaultSchemaConfiguration();
        }

        public void setSchemaConfiguration(SchemaConfiguration schemaConfiguration)
        {
            this.schemaConfiguration = schemaConfiguration;
        }
    }
}
