package net.java.ao.it.model.backup;

import net.java.ao.Entity;

/**
 * This entity is referenced in {@link net.java.ao.it.model.backup.EntityWithManyToOne}.
 */
public interface EntityWithName extends Entity
{
    String getName();

    void setName(String name);
}
