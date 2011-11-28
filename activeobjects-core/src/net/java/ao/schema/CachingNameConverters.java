package net.java.ao.schema;

import static com.google.common.base.Preconditions.*;

public final class CachingNameConverters implements NameConverters
{
    private final TableNameConverter tableNameConverter;
    private final FieldNameConverter fieldNameConverter;
    private final SequenceNameConverter sequenceNameConverter;
    private final TriggerNameConverter triggerNameConverter;
    private final IndexNameConverter indexNameConverter;
    private final UniqueNameConverter uniqueNameConverter;

    public CachingNameConverters(NameConverters nameConverters)
    {
        checkNotNull(nameConverters);
        this.tableNameConverter = new CachingTableNameConverter(nameConverters.getTableNameConverter());
        this.fieldNameConverter = nameConverters.getFieldNameConverter();
        this.sequenceNameConverter = nameConverters.getSequenceNameConverter();
        this.triggerNameConverter = nameConverters.getTriggerNameConverter();
        this.indexNameConverter = nameConverters.getIndexNameConverter();
        this.uniqueNameConverter = nameConverters.getUniqueNameConverter();
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
