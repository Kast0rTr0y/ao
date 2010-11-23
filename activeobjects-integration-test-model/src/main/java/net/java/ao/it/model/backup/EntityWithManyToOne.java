package net.java.ao.it.model.backup;

import net.java.ao.Entity;

public interface EntityWithManyToOne extends Entity
{
    EntityWithName getEntityWithName();

    void setEntityWithName(EntityWithName entityWithName);
}
