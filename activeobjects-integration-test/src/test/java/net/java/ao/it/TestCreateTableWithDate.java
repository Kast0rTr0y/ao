package net.java.ao.it;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.schema.Default;

import java.util.Date;
import java.util.List;

public final class TestCreateTableWithDate extends AbstractTestCreateTable {
    @Override
    protected List<Class<? extends Entity>> getEntities() {
        return ImmutableList.of(
                EntityWithDate.class,
                EntityWithDateAndDefaultValue.class
        );
    }

    public static interface EntityWithDate extends Entity {
        Date getField();

        void setField(Date field);
    }

    public static interface EntityWithDateAndDefaultValue extends Entity {
        @Default("2011-11-26 13:49:57")
        Date getField();

        void setField(Date field);
    }
}
