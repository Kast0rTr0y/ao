package net.java.ao.schema;

public final class DefaultIndexNameConverter implements IndexNameConverter
{
    @Override
    public String getName(String tableName, String fieldName)
    {
        return "index_" + tableName.toLowerCase() + "_" + fieldName.toLowerCase();
    }
}
