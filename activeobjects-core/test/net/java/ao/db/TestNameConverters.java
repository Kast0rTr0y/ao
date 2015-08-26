package net.java.ao.db;

import net.java.ao.schema.Case;
import net.java.ao.schema.DefaultIndexNameConverter;
import net.java.ao.schema.DefaultSequenceNameConverter;
import net.java.ao.schema.DefaultTriggerNameConverter;
import net.java.ao.schema.DefaultUniqueNameConverter;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.schema.UnderscoreFieldNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;
import net.java.ao.schema.UniqueNameConverter;

public class TestNameConverters implements NameConverters {
    @Override
    public TableNameConverter getTableNameConverter() {
        return new UnderscoreTableNameConverter(Case.LOWER);
    }

    @Override
    public FieldNameConverter getFieldNameConverter() {
        return new UnderscoreFieldNameConverter(Case.LOWER);
    }

    @Override
    public SequenceNameConverter getSequenceNameConverter() {
        return new DefaultSequenceNameConverter();
    }

    @Override
    public TriggerNameConverter getTriggerNameConverter() {
        return new DefaultTriggerNameConverter();
    }

    @Override
    public IndexNameConverter getIndexNameConverter() {
        return new DefaultIndexNameConverter();
    }

    @Override
    public UniqueNameConverter getUniqueNameConverter() {
        return new DefaultUniqueNameConverter();
    }
}
