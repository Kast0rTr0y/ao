package net.java.ao.schema;

public interface IndexNameConverter {
    String getName(String tableName, String fieldName);

    String getPrefix(String tableName);
}
