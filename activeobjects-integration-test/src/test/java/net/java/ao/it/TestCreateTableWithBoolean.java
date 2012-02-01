package net.java.ao.it;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.schema.Default;

import java.util.List;

public final class TestCreateTableWithBoolean extends AbstractTestCreateTable
{
    @Override
    protected List<Class<? extends Entity>> getEntities()
    {
        return ImmutableList.of(
                EntityWithBoolean.class,
                EntityWithBooleanAndDefaultValue.class,
                EntityWithBooleanAndDefaultValueFalse.class);
    }

    private static interface EntityWithBoolean extends Entity
    {
        boolean getField();

        void setField(boolean field);
    }

    private static interface EntityWithBooleanAndDefaultValue extends Entity
    {
        @Default("true")
        boolean getField();

        void setField(boolean field);
    }

    private static interface EntityWithBooleanAndDefaultValueFalse extends Entity
    {
        @Default("false")
        boolean getField();

        void setField(boolean field);
    }
}