package net.java.ao.schema;

import net.java.ao.RawEntity;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class CanonicalClassNameTableNameConverter implements TableNameConverter
{
    public final String getName(Class<? extends RawEntity<?>> entityClass)
    {
        return getName(checkNotNull(entityClass).getCanonicalName());
    }

    protected abstract String getName(String entityClassCanonicalName);
}