package net.java.ao.it.model;

import net.java.ao.Entity;

public interface Chair extends Entity
{
    String getColour();
    void setColour(String colour);
}