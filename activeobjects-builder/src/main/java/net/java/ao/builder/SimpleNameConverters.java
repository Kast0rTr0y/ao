package net.java.ao.builder;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UniqueNameConverter;

import static com.google.common.base.Preconditions.*;

public final class SimpleNameConverters implements NameConverters
{
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final SequenceNameConverter sequenceNameConverter;
    private final TriggerNameConverter triggerNameConverter;
    private final IndexNameConverter indexNameConverter;
    private final UniqueNameConverter uniqueNameConverter;

    public SimpleNameConverters(
            TableNameConverter tableNameConverter,
            FieldNameConverter fieldNameConverter,
            SequenceNameConverter sequenceNameConverter,
            TriggerNameConverter triggerNameConverter,
            IndexNameConverter indexNameConverter,
            UniqueNameConverter uniqueNameConverter)
    {
        this.tableNameConverter = checkNotNull(tableNameConverter);
        this.fieldNameConverter = checkNotNull(fieldNameConverter);
        this.sequenceNameConverter = checkNotNull(sequenceNameConverter);
        this.triggerNameConverter = checkNotNull(triggerNameConverter);
        this.indexNameConverter = checkNotNull(indexNameConverter);
        this.uniqueNameConverter = checkNotNull(uniqueNameConverter);
    }

    @Override
    public TableNameConverter getTableNameConverter()
    {
        return tableNameConverter;
    }

    @Override
    public FieldNameConverter getFieldNameConverter()
    {
        return fieldNameConverter;
    }

    @Override
    public SequenceNameConverter getSequenceNameConverter()
    {
        return sequenceNameConverter;
    }

    @Override
    public TriggerNameConverter getTriggerNameConverter()
    {
        return triggerNameConverter;
    }

    @Override
    public IndexNameConverter getIndexNameConverter()
    {
        return indexNameConverter;
    }

    @Override
    public UniqueNameConverter getUniqueNameConverter()
    {
        return uniqueNameConverter;
    }
}
