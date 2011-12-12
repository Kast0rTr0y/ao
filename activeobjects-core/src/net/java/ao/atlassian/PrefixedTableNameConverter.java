package net.java.ao.atlassian;


import net.java.ao.RawEntity;
import net.java.ao.schema.TableNameConverter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>A {@link net.java.ao.schema.TableNameConverter table name converter} that will prepend the given {@link Prefix} to table names.</p>
 * <p>It uses a {@link net.java.ao.schema.TableNameConverter delegate table name converter} for the <em>general</em> conversion strategy.</p>
 */
final class PrefixedTableNameConverter implements TableNameConverter
{
    private final TablePrefix prefix;

    /**
     * The table name converter we delegate the real conversion to
     */
    private final TableNameConverter delegate;

    public PrefixedTableNameConverter(TablePrefix prefix, TableNameConverter delegate)
    {
        this.prefix = checkNotNull(prefix);
        this.delegate = checkNotNull(delegate);
    }

    public String getName(Class<? extends RawEntity<?>> clazz)
    {
        return prefix.prepend(delegate.getName(clazz));
    }
}
