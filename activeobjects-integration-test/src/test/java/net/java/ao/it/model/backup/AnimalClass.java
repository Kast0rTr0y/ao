package net.java.ao.it.model.backup;

import net.java.ao.Entity;

/**
 * A class of animals, such as 'mammals' for example
 */
public interface AnimalClass extends Entity
{
    String getName();

    void setName(String name);
}
