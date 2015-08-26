package net.java.ao.benchmark.model;

import net.java.ao.Entity;

public interface Person extends Entity {
    public String getFirstName();

    public void setFirstName(String firstName);

    public String getLastName();

    public void setLastName(String lastName);
}
