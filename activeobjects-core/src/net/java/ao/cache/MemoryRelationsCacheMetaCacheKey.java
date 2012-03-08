package net.java.ao.cache;

import net.java.ao.RawEntity;

final class MemoryRelationsCacheMetaCacheKey
{
    private final RawEntity<?> entity;
    private final String field;

    public MemoryRelationsCacheMetaCacheKey(RawEntity<?> entity, String field)
    {
        this.entity = entity;
        this.field = field;
    }

    public RawEntity<?> getEntity()
    {
        return entity;
    }

    public String getField()
    {
        return field;
    }

    @Override
    public String toString()
    {
        return entity.toString() + "; " + field;
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

        final MemoryRelationsCacheMetaCacheKey that = (MemoryRelationsCacheMetaCacheKey) o;

        if (entity != null ? !entity.equals(that.entity) : that.entity != null)
        {
            return false;
        }
        if (field != null ? !field.equals(that.field) : that.field != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = entity != null ? entity.hashCode() : 0;
        result = 31 * result + (field != null ? field.hashCode() : 0);
        return result;
    }
}
