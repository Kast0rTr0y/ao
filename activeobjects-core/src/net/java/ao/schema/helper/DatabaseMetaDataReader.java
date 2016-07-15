package net.java.ao.schema.helper;

import net.java.ao.RawEntity;

import java.sql.DatabaseMetaData;

public interface DatabaseMetaDataReader {

    /**
     * Checks if the table corresponding to given type exists in the database.
     *
     * @param databaseMetaData of the database to be checked
     * @param type to check against
     * @return true if the table exists, false otherwise
     */
    public boolean isTablePresent(DatabaseMetaData databaseMetaData, Class<? extends RawEntity<?>> type);

    /**
     * Gets the names of the existing tables in the DB
     *
     * @param databaseMetaData the meta data from which to extract the table names
     * @return an {@link Iterable} of table names as {@link String strings}
     */
    Iterable<String> getTableNames(DatabaseMetaData databaseMetaData);

    /**
     * @param databaseMetaData
     * @param tableName
     * @return
     */
    Iterable<? extends Field> getFields(DatabaseMetaData databaseMetaData, String tableName);

    /**
     * Gets the list of foreign keys for a given table
     *
     * @param databaseMetaData the database metadata to read the information from
     * @param tableName        the name of the table from which to read the foreign keys
     * @return an {@link Iterable} of foreign keys
     */
    Iterable<? extends ForeignKey> getForeignKeys(DatabaseMetaData databaseMetaData, String tableName);

    /**
     * Gets the list of simple indexes for a given table. AO only supports single column indexes, and as
     * such composite indexes will not be returned by this method even if they exist in the table.
     * Prior to AO v1.1.4 this method returned fragments of composite indexes and it was then possible for
     * the migration code to crash because it would try to delete the same index multiple times.
     *
     * @param databaseMetaData the database metadata to read the information from
     * @param tableName        the name of the table from which to read the indexes
     * @return an {@link Iterable} of simple indexes
     */
    Iterable<? extends Index> getIndexes(DatabaseMetaData databaseMetaData, String tableName);
}
