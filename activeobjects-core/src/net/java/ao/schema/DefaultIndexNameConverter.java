package net.java.ao.schema;

public final class DefaultIndexNameConverter implements IndexNameConverter {
    @Override
    public String getName(String tableName, String fieldName) {
        return getPrefix(tableName) +
                "_" +
                Case.LOWER.apply(fieldName);
    }

    @Override
    public String getPrefix(String tableName) {
        return "index_" +
                Case.LOWER.apply(tableName);
    }
}
