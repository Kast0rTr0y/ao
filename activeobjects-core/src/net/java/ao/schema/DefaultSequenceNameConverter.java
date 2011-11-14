package net.java.ao.schema;

public final class DefaultSequenceNameConverter implements SequenceNameConverter
{
    @Override
    public String getName(String tableName, String fieldName)
    {
        return new StringBuilder()
                .append(tableName)
                .append('_')
                .append(fieldName)
                .append("_SEQ")
                .toString();
    }
}
