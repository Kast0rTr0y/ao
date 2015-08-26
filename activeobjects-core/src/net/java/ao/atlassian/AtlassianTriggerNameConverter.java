package net.java.ao.atlassian;

import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.TriggerNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.shorten;

public final class AtlassianTriggerNameConverter implements TriggerNameConverter {
    private final TriggerNameConverter delegate;

    public AtlassianTriggerNameConverter() {
        this(new DefaultTriggerNameConverter());
    }

    public AtlassianTriggerNameConverter(TriggerNameConverter delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public String autoIncrementName(String tableName, String columnName) {
        return shorten(delegate.autoIncrementName(tableName, columnName), ConverterUtils.MAX_LENGTH);
    }
}
