package net.java.ao.schema.info;

import net.java.ao.schema.NameConverters;
import net.java.ao.types.TypeManager;

public interface TableInfoResolverFactory
{

    TableInfoResolver create(NameConverters nameConverters, TypeManager typeManager);

}
