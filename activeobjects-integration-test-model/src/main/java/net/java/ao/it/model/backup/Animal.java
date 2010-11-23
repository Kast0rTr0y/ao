package net.java.ao.it.model.backup;

import net.java.ao.Entity;

/** An animal */
public interface Animal extends Entity
{
    String getName();

    void setName(String name);

    AnimalClass getAnimalClass();

    void setAnimalClass(AnimalClass animalClass);
}
