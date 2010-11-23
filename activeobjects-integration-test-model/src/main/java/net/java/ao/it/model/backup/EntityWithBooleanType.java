package net.java.ao.it.model.backup;

import net.java.ao.Entity;

public interface EntityWithBooleanType extends Entity
{
    boolean isCondition();

    void setCondition(boolean condition);
}
