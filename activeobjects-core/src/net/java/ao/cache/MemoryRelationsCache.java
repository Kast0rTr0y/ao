/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.cache;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import net.java.ao.RawEntity;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class MemoryRelationsCache implements RelationsCache
{
    private final Map<MemoryRelationsCacheKey, RawEntity<?>[]> cache;
    private final Multimap<Class<? extends RawEntity<?>>, MemoryRelationsCacheKey> typeMap;
    private final Multimap<MemoryRelationsCacheMetaCacheKey, MemoryRelationsCacheKey> fieldMap;

    MemoryRelationsCache()
    {
        cache = new HashMap<MemoryRelationsCacheKey, RawEntity<?>[]>();
        typeMap = HashMultimap.create();
        fieldMap = HashMultimap.create();
    }

    @Override
    public <T extends RawEntity<?>> T[] get(RawEntity<?> from, Class<T> toType, Class<? extends RawEntity<?>> throughType, String[] fields, String where)
    {
        return (T[]) cache.get(new MemoryRelationsCacheKey(from, toType, throughType, fields, where));
    }

    @Override
    public void put(RawEntity<?> from, RawEntity<?>[] through, Class<? extends RawEntity<?>> throughType, RawEntity<?>[] to, Class<? extends RawEntity<?>> toType, String[] fields, String where)
    {
        final MemoryRelationsCacheKey key = new MemoryRelationsCacheKey(from, toType, throughType, fields, where);

        cache.put(key, to);
        typeMap.put(key.getThroughType(), key);

        for (String field : fields)
        {
            for (RawEntity<?> entity : through)
            {
                fieldMap.put(new MemoryRelationsCacheMetaCacheKey(entity, field), key);
            }
        }
    }

    @Override
    public void remove(Class<? extends RawEntity<?>>... types)
    {
        for (Class<? extends RawEntity<?>> type : types)
        {
            Collection<MemoryRelationsCacheKey> keys = typeMap.get(type);
            if (keys != null)
            {
                for (MemoryRelationsCacheKey key : keys)
                {
                    cache.remove(key);
                }
                typeMap.removeAll(type);
            }
        }
    }

    @Override
    public void remove(RawEntity<?> entity, String[] fields)
    {
        for (String field : fields)
        {
            final Collection<MemoryRelationsCacheKey> keys = fieldMap.get(new MemoryRelationsCacheMetaCacheKey(entity, field));
            if (keys != null)
            {
                for (MemoryRelationsCacheKey key : keys)
                {
                    cache.remove(key);
                }
            }
        }
    }

    @Override
    public void flush()
    {
        cache.clear();
        typeMap.clear();
        fieldMap.clear();
    }
}
