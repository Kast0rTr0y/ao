package net.java.ao.builder;

import net.java.ao.DefaultSchemaConfiguration;
import net.java.ao.EntityManager;
import net.java.ao.EntityManagerConfiguration;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.CamelCaseFieldNameConverter;
import net.java.ao.schema.CamelCaseTableNameConverter;
import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableAnnotationTableNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;

import static com.google.common.base.Preconditions.*;

public abstract class AbstractEntityManagerBuilderWithDatabaseProperties<B extends AbstractEntityManagerBuilderWithDatabaseProperties>
{
    private final BuilderDatabaseProperties databaseProperties;
    private final BuilderEntityManagerConfiguration configuration;

    AbstractEntityManagerBuilderWithDatabaseProperties(BuilderDatabaseProperties databaseProperties)
    {
        this(databaseProperties, new BuilderEntityManagerConfiguration());
    }

    AbstractEntityManagerBuilderWithDatabaseProperties(BuilderDatabaseProperties databaseProperties, BuilderEntityManagerConfiguration configuration)
    {
        this.databaseProperties = checkNotNull(databaseProperties);
        this.configuration = checkNotNull(configuration);
    }

    public B schema(String schema)
    {
        databaseProperties.setSchema(schema);
        return cast();
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

    public B sequenceNameConverter(SequenceNameConverter sequenceNameConverter)
    {
        configuration.setSequenceNameConverter(checkNotNull(sequenceNameConverter));
        return cast();
    }

    public B triggerNameConverter(TriggerNameConverter triggerNameConverter)
    {
        configuration.setTriggerNameConverter(checkNotNull(triggerNameConverter));
        return cast();
    }

    public B indexNameConverter(IndexNameConverter indexNameConverter)
    {
        configuration.setIndexNameConverter(checkNotNull(indexNameConverter));
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

    final BuilderDatabaseProperties getDatabaseProperties()
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
        private SequenceNameConverter sequenceNameConverter;
        private TriggerNameConverter triggerNameConverter;
        private IndexNameConverter indexNameConverter;

        private boolean useWeakCache = false;

        @Override
        public boolean useWeakCache()
        {
            return useWeakCache;
        }

        @Override
        public NameConverters getNameConverters()
        {
            return new SimpleNameConverters(
                    getTableNameConverter(),
                    getFieldNameConverter(),
                    getSequenceNameConverter(),
                    getTriggerNameConverter(),
                    getIndexNameConverter());
        }

        private TableNameConverter getTableNameConverter()
        {
            return tableNameConverter != null ? tableNameConverter : defaultTableNameConverter();
        }

        private static TableNameConverter defaultTableNameConverter()
        {
            return new TableAnnotationTableNameConverter(new CamelCaseTableNameConverter());
        }

        private SequenceNameConverter getSequenceNameConverter()
        {
            return sequenceNameConverter != null ? sequenceNameConverter : defaultSequenceNameConverter();
        }

        private TriggerNameConverter getTriggerNameConverter()
        {
            return triggerNameConverter != null ? triggerNameConverter : defaultTriggerNameConverter();
        }

        private IndexNameConverter getIndexNameConverter()
        {
            return indexNameConverter != null ? indexNameConverter : defaultIndexNameConverter();
        }

        private IndexNameConverter defaultIndexNameConverter()
        {
            return new DefaultIndexNameConverter();
        }

        private TriggerNameConverter defaultTriggerNameConverter()
        {
            return new DefaultTriggerNameConverter();
        }

        private SequenceNameConverter defaultSequenceNameConverter()
        {
            return new DefaultSequenceNameConverter();
        }

        private FieldNameConverter getFieldNameConverter()
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

        public void setSequenceNameConverter(SequenceNameConverter sequenceNameConverter)
        {
            this.sequenceNameConverter = sequenceNameConverter;
        }

        public void setTriggerNameConverter(TriggerNameConverter triggerNameConverter)
        {
            this.triggerNameConverter = triggerNameConverter;
        }

        public void setIndexNameConverter(IndexNameConverter indexNameConverter)
        {
            this.indexNameConverter = indexNameConverter;
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
