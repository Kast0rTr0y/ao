package net.java.ao.schema;

public interface TriggerNameConverter
{
    String onUpdateName(String tableName, String fieldName);

    String autoIncrementName(String tableName, String fieldName);
}
