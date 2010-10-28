package net.java.ao.schema.helper;

public interface ForeignKey
{
    String getLocalTableName();

    String getLocalFieldName();

    String getForeignTableName();

    String getForeignFieldName();
}
