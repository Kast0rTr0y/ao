package net.java.ao.schema.info;

import net.java.ao.RawEntity;
import net.java.ao.schema.NameConverters;

public interface SchemaInfoResolver
{

    <T extends RawEntity<?>> SchemaInfo<T> resolve(NameConverters nameConverters, Class<T> type);

}
