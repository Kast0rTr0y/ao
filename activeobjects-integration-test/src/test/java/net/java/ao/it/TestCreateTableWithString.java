package net.java.ao.it;

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.schema.Default;

import java.util.List;

public final class TestCreateTableWithString extends AbstractTestCreateTable
{
    @Override
    protected List<Class<? extends Entity>> getEntities()
    {
        return ImmutableList.of(
                EntityWithString.class,
                EntityWithStringAndDefaultValue.class
        );
    }

    private static interface EntityWithString extends Entity
    {
        String getField();

        void setField(String field);
    }

    private static interface EntityWithStringAndDefaultValue extends Entity
    {
        @Default("some-default")
        String getField();

        void setField(String field);
    }
}