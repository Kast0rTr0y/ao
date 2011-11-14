package net.java.ao.builder;

import com.google.common.base.Preconditions;
import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;

import static com.google.common.base.Preconditions.*;

public final class SimpleNameConverters implements NameConverters
{
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final SequenceNameConverter sequenceNameConverter;
    private final TriggerNameConverter triggerNameConverter;
    private final IndexNameConverter indexNameConverter;

    public SimpleNameConverters(
            TableNameConverter tableNameConverter,
            FieldNameConverter fieldNameConverter,
            SequenceNameConverter sequenceNameConverter,
            TriggerNameConverter triggerNameConverter,
            IndexNameConverter indexNameConverter)
    {
        this.tableNameConverter = checkNotNull(tableNameConverter);
        this.fieldNameConverter = checkNotNull(fieldNameConverter);
        this.sequenceNameConverter = checkNotNull(sequenceNameConverter);
        this.triggerNameConverter = checkNotNull(triggerNameConverter);
        this.indexNameConverter = checkNotNull(indexNameConverter);
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
}
