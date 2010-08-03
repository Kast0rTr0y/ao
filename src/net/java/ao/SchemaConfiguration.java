package net.java.ao;

/**
 * <p>This interface represents the configuration to be used when Active Objects works with database schemas. It includes
 * constraints that Active Objects should work with when dealing with schemas. For example when using
 * {@link net.java.ao.EntityManager#migrate(Class[])}</p>
 * <p>For example the {@link #shouldManageTable(String)} tells Active Objects whether it is allowed to update/delete a given table
 * present in the database.</p>
 *
 * @see net.java.ao.EntityManager#migrate(Class[])
 */
public interface SchemaConfiguration
{
    /**
     * Tells whether the table with the given table name should be managed by Active Objects.
     *
     * @param tableName the name of the table to be managed (or not)
     * @return {@code true} if Active Objects is allowed to manage the table named {@code tableName}
     */
    boolean shouldManageTable(String tableName);
}
