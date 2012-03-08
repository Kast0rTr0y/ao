package net.java.ao.cache;

import net.java.ao.RawEntity;

import java.util.Arrays;

final class MemoryRelationsCacheKey
{
    private final RawEntity<?> from;
    private final Class<? extends RawEntity<?>> toType;
    private final Class<? extends RawEntity<?>> throughType;
    private final String[] fields;
    private final String where;

    public MemoryRelationsCacheKey(RawEntity<?> from, Class<? extends RawEntity<?>> toType,
                                   Class<? extends RawEntity<?>> throughType, String[] fields, String where)
    {
        this.from = from;
        this.toType = toType;
        this.throughType = throughType;
        this.fields = sortFields(fields);
        this.where = where;
    }

    public RawEntity<?> getFrom()
    {
        return from;
    }

    public Class<? extends RawEntity<?>> getToType()
    {
        return toType;
    }

    public String[] getFields()
    {
        return fields;
    }

    public Class<? extends RawEntity<?>> getThroughType()
    {
        return throughType;
    }

    @Override
    public String toString()
    {
        return '(' + from.toString() + "; to=" + toType.getName() + "; through=" + throughType.getName() + "; " + Arrays.toString(fields) + ')';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final MemoryRelationsCacheKey cacheKey = (MemoryRelationsCacheKey) o;

        if (!Arrays.equals(fields, cacheKey.fields))
        {
            return false;
        }
        if (from != null ? !from.equals(cacheKey.from) : cacheKey.from != null)
        {
            return false;
        }
        if (throughType != null ? !throughType.equals(cacheKey.throughType) : cacheKey.throughType != null)
        {
            return false;
        }
        if (toType != null ? !toType.equals(cacheKey.toType) : cacheKey.toType != null)
        {
            return false;
        }
        if (where != null ? !where.equals(cacheKey.where) : cacheKey.where != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (toType != null ? toType.hashCode() : 0);
        result = 31 * result + (throughType != null ? throughType.hashCode() : 0);
        result = 31 * result + (fields != null ? Arrays.hashCode(fields) : 0);
        result = 31 * result + (where != null ? where.hashCode() : 0);
        return result;
    }

    private static String[] sortFields(String[] fields)
    {
        Arrays.sort(fields);
        return fields;
    }
}
