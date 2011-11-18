package net.java.ao.it;

import net.java.ao.RawEntity;
import net.java.ao.schema.TableNameConverter;

public final class TestTableNameConverter implements TableNameConverter
{
    @Override
    public String getName(Class<? extends RawEntity<?>> clazz)
    {
        return "TEST_ENTITY";
    }
}
