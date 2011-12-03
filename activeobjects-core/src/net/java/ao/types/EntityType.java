package net.java.ao.types;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.sql.Types.INTEGER;

import net.java.ao.Common;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;

final class EntityType<K, T extends RawEntity<K>> extends AbstractLogicalType<T>
{
    private final TypeInfo<K> primaryKeyTypeInfo;
    private final Class<K> primaryKeyClass;
    
    public EntityType(Class<T> entityClass,
                      TypeInfo<K> primaryKeyTypeInfo,
                      Class<K> primaryKeyClass)
    {
        super("Entity(" + entityClass.getName() + ")",
              new Class<?>[] { RawEntity.class },
              INTEGER, new Integer[] { });
        this.primaryKeyTypeInfo = primaryKeyTypeInfo;
        this.primaryKeyClass = primaryKeyClass;
    }
    
    @Override
    public int getDefaultJdbcWriteType()
    {
        return primaryKeyTypeInfo.getJdbcWriteType();
    }
    
    @Override
    public Object validate(Object value)
    {
        // We currently support passing the primary key type as a database parameter in place of the
        // entity, so we need to bypass the regular type validation here.
        return value;
    }
    
    @Override
    public T pullFromDatabase(EntityManager manager, ResultSet res, Class<T> type, String columnName)
        throws SQLException
    {
        return Common.createPeer(manager, type,
                                 primaryKeyTypeInfo.getLogicalType().pullFromDatabase(manager, res, primaryKeyClass, columnName));
    }

    @Override
    public void putToDatabase(EntityManager manager, PreparedStatement stmt, int index, T value, int jdbcType)
        throws SQLException
    {
        primaryKeyTypeInfo.getLogicalType().putToDatabase(manager, stmt, index, Common.getPrimaryKeyValue(value),
                                                          primaryKeyTypeInfo.getJdbcWriteType());
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (other instanceof EntityType)
        {
            EntityType<?, ?> et = (EntityType<?, ?>) other;
            return et.primaryKeyTypeInfo.equals(primaryKeyTypeInfo)
                && et.primaryKeyClass.equals(primaryKeyClass);
        }
        return false;
    }
}