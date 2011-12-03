package net.java.ao.schema.helper;

import net.java.ao.types.TypeInfo;

public interface Field
{
    String getName();

    TypeInfo<?> getDatabaseType();

    int getJdbcType();

    boolean isAutoIncrement();

    boolean isNotNull();

    Object getDefaultValue();

    boolean isPrimaryKey();

    boolean isUnique();
}
