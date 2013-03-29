package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

public interface TableInfoResolver
{

    <T extends RawEntity<K>, K> TableInfo<T, K> resolve(Class<T> type);

}
