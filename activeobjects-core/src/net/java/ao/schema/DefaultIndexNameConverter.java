package net.java.ao.schema;

public final class DefaultIndexNameConverter implements IndexNameConverter
{
    @Override
    public String getName(String tableName, String fieldName)
    {
        return new StringBuilder()
                .append("index_")
                .append(Case.LOWER.apply(tableName))
                .append("_")
                .append(Case.LOWER.apply(fieldName))
                .toString();
    }
}
