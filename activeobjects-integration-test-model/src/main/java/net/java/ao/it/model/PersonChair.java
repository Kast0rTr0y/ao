package net.java.ao.it.model;

import net.java.ao.Entity;

public interface PersonChair extends Entity {
    Person getPerson();

    void setPerson(Person person);

    Chair getChair();

    void setChair(Chair chair);
}
