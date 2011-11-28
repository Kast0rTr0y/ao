package net.java.ao.schema.helper;

import net.java.ao.types.DatabaseType;

public interface Field
{
    String getName();

    DatabaseType<?> getDatabaseType();

    int getPrecision();

    int getScale();

    boolean isAutoIncrement();

    boolean isNotNull();

    Object getDefaultValue();

    boolean isPrimaryKey();

    boolean isUnique();
}
