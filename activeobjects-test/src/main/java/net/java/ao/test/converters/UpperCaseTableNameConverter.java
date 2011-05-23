package net.java.ao.test.converters;

import net.java.ao.RawEntity;
import net.java.ao.schema.Case;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;

public final class UpperCaseTableNameConverter implements TableNameConverter
{
    private final TableNameConverter tnc = new UnderscoreTableNameConverter(Case.UPPER);

    @Override
    public String getName(Class<? extends RawEntity<?>> clazz)
    {
        return tnc.getName(clazz);
    }
}
