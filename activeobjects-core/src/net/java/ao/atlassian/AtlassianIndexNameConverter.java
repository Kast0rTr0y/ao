package net.java.ao.atlassian;

import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.IndexNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.prefix;
import static net.java.ao.Common.shorten;

public final class AtlassianIndexNameConverter implements IndexNameConverter {
    private final IndexNameConverter delegate;

    public AtlassianIndexNameConverter() {
        this(new DefaultIndexNameConverter());
    }

    public AtlassianIndexNameConverter(IndexNameConverter delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public String getName(String tableName, String columnName) {
        return shorten(delegate.getName(tableName, columnName), ConverterUtils.MAX_LENGTH);
    }

    @Override
    public String getPrefix(String tableName) {
        return prefix(delegate.getPrefix(tableName), ConverterUtils.MAX_LENGTH);
    }
}
