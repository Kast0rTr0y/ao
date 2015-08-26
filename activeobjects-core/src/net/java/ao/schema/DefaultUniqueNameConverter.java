package net.java.ao.schema;

public final class DefaultUniqueNameConverter implements UniqueNameConverter {
    @Override
    public String getName(String tableName, String fieldName) {
        return "U_" + tableName + '_' + fieldName;
    }
}
