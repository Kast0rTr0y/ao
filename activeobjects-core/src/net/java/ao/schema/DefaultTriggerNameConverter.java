package net.java.ao.schema;

public final class DefaultTriggerNameConverter implements TriggerNameConverter
{
    @Override
    public String onUpdateName(String tableName, String fieldName)
    {
        return tableName + '_' + fieldName + "_onupdate";
    }

    @Override
    public String autoIncrementName(String tableName, String fieldName)
    {
        return tableName + '_' + fieldName + "_autoinc";
    }
}
