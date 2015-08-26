package net.java.ao.it;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.schema.Default;

import java.util.List;

public final class TestCreateTableWithInteger extends AbstractTestCreateTable {
    @Override
    protected List<Class<? extends Entity>> getEntities() {
        return ImmutableList.of(
                EntityWithInteger.class,
                EntityWithIntegerAndDefaultValue.class
        );
    }

    public static interface EntityWithInteger extends Entity {
        int getField();

        void setField(int field);
    }

    public static interface EntityWithIntegerAndDefaultValue extends Entity {
        @Default("13")
        int getField();

        void setField(int field);
    }
}