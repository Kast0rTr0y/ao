package net.java.ao.atlassian;

import net.java.ao.schema.DefaultUniqueNameConverter;
import net.java.ao.schema.UniqueNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.shorten;

public final class AtlassianUniqueNameConverter implements UniqueNameConverter
{
    private final UniqueNameConverter delegate;

    public AtlassianUniqueNameConverter()
    {
        this(new DefaultUniqueNameConverter());
    }

    public AtlassianUniqueNameConverter(UniqueNameConverter delegate)
    {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public String getName(String tableName, String columnName)
    {
        return shorten(delegate.getName(tableName, columnName), ConverterUtils.MAX_LENGTH);
    }
}
