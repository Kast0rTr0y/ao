package net.java.ao.it.model;

import net.java.ao.Entity;

public interface PersonChair extends Entity
{
    public Person getPerson();
    public void setPerson(Person person);

    public Chair getChair();
    public void setChair(Chair chair);
}
