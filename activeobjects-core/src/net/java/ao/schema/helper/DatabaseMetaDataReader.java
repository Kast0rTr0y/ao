package net.java.ao.schema.helper;

import java.sql.DatabaseMetaData;

public interface DatabaseMetaDataReader {
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
     * Gets the list of indexes for a given table
     *
     * @param databaseMetaData the database metadata to read the information from
     * @param tableName        the name of the table from which to read the indexes
     * @return an {@link Iterable} of indexes
     */
    Iterable<? extends Index> getIndexes(DatabaseMetaData databaseMetaData, String tableName);
}
