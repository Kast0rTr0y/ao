package net.java.ao.it.model.backup;

import net.java.ao.Entity;
import net.java.ao.schema.SQLType;

public interface EntityWithStringType extends Entity
{
    String getString();

    @SQLType(precision = 301)
    void setString(String string);
}
