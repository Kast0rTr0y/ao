package net.java.ao.schema.helper;

import java.util.Collection;

public interface Index {
    String getTableName();

    Collection<String> getFieldNames();

    String getIndexName();
}
