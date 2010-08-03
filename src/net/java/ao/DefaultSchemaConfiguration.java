package net.java.ao;

/**
 * Default implementation of {@link net.java.ao.SchemaConfiguration} that doesn't have any constraints regarding the
 * database schema.
 */
public class DefaultSchemaConfiguration implements SchemaConfiguration
{
    /**
     * Always returns {@code true}
     *
     * @param tableName the name of the table to be managed (or not)
     * @return {@code true}
     */
    public boolean shouldManageTable(String tableName)
    {
        return true;
    }
}
