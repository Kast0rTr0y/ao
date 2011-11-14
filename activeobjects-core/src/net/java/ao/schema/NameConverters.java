package net.java.ao.schema;

public interface NameConverters
{
    TableNameConverter getTableNameConverter();

    FieldNameConverter getFieldNameConverter();

    SequenceNameConverter getSequenceNameConverter();

    TriggerNameConverter getTriggerNameConverter();

    IndexNameConverter getIndexNameConverter();
}
