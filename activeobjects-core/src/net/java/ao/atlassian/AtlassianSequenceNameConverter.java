package net.java.ao.atlassian;

import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.SequenceNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.shorten;

public final class AtlassianSequenceNameConverter implements SequenceNameConverter
{
    private final SequenceNameConverter delegate;

    public AtlassianSequenceNameConverter()
    {
        this(new DefaultSequenceNameConverter());
    }

    public AtlassianSequenceNameConverter(SequenceNameConverter delegate)
    {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public String getName(String tableName, String columnName)
    {
        return shorten(delegate.getName(tableName, columnName), ConverterUtils.MAX_LENGTH);
    }
}
