package net.java.ao.test.config;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;

public interface Parameters
{
    TableNameConverter getTableNameConverter();

    FieldNameConverter getFieldNameConverter();
}
