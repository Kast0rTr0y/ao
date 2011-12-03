package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.google.common.collect.ImmutableSet;

import net.java.ao.EntityManager;

/**
 * Defines a logical type which can translate between one or more Java types and one or more JDBC types.
 * This should be independent of the underlying database dialect.
 */
public interface LogicalType<T>
{
    /**
     * Returns a descriptive name for this logical type.
     */
    String getName();
    
    /**
     * Returns the Java types that are handled by this logical type.  Only one logical type can handle
     * any given Java type.
     */
    ImmutableSet<Class<?>> getTypes();
 
    /**
     * Returns the set of JDBC types that can be translated into this logical type.   
     */
    ImmutableSet<Integer> getJdbcReadTypes();
    
    /**
     * Returns the default JDBC type to use when writing values to the database.  This can be overridden
     * by each database provider in the {@link SchemaProperties}.
     */
    int getDefaultJdbcWriteType();
    
    /**
     * Returns a boolean specifying if this type can be used as a primary key value.
     * @return if it's allowed or not
     */
    boolean isAllowedAsPrimaryKey();
    
    /**
     * Attempts to parse a string into a value of this logical type.
     */
    T parse(String input) throws IllegalArgumentException;
    
    /**
     * Same as {@link #parse(String)}, but adds any necessary validation when the value is being
     * used as a default value for a column.
     */
    T parseDefault(String input) throws IllegalArgumentException;
    
    /**
     * Verifies that a value can be stored in the database.  Returns the
     * same value or a canonically equivalent value.
     */
    Object validate(Object value) throws IllegalArgumentException;
    
    /**
     * Sets a value in a JDBC statement.
     */
    void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, T value, int jdbcType) throws SQLException;
    
    /**
     * Reads a value from a JDBC result set.
     */
    T pullFromDatabase(EntityManager manager, ResultSet res, Class<T> type, String columnName) throws SQLException;
    
    /**
     * Reads a value from a JDBC result set.
     */
    T pullFromDatabase(EntityManager manager, ResultSet res, Class<T> type, int columnIndex) throws SQLException;

    boolean shouldCache(Class<?> value);
    
    boolean valueEquals(Object val1, Object val2);
    
    String valueToString(T value);
}
