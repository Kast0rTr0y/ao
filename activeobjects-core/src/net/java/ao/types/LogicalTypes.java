package net.java.ao.types;

import java.net.URI;
import java.net.URL;
import java.util.Date;

import net.java.ao.RawEntity;

public abstract class LogicalTypes
{
    public static LogicalType<Integer> integerType()
    {
        return new IntegerType();
    }

    public static LogicalType<Long> longType()
    {
        return new LongType();
    }
    
    public static LogicalType<Double> doubleType()
    {
        return new DoubleType();
    }
    
    public static LogicalType<Float> floatType()
    {
        return new FloatType();
    }
    
    public static LogicalType<Boolean> booleanType()
    {
        return new BooleanType();
    }
    
    public static LogicalType<String> stringType()
    {
        return new StringType();
    }
    
    public static LogicalType<Object> blobType()
    {
        return new BlobType();
    }
    
    public static LogicalType<Enum<?>> enumType()
    {
        return new EnumType();
    }
    
    public static LogicalType<Date> dateType()
    {
        return new DateType();
    }
    
    public static LogicalType<URI> uriType()
    {
        return new URIType();
    }
    
    public static LogicalType<URL> urlType()
    {
        return new URLType();
    }
    
    public static <K, T extends RawEntity<K>> LogicalType<T> entityType(Class<T> entityClass,
        TypeInfo<K> primaryKeyTypeInfo,
        Class<K> primaryKeyClass)
    {
        return new EntityType<K, T>(entityClass, primaryKeyTypeInfo, primaryKeyClass);
    }
}
