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

import net.java.ao.RawEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author Daniel Spiewak
 */
public final class RAMRelationsCache implements RelationsCache
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final RelationsCache cache;

    public RAMRelationsCache()
    {
        cache = new ConcurrentRelationsCache(new MemoryRelationsCache());
    }

    @Override
    public <T extends RawEntity<K>, K> T[] get(RawEntity<?> from, Class<T> toType, Class<? extends RawEntity<?>> throughType, String[] fields, String where)
    {
        final T[] ts = cache.get(from, toType, throughType, fields, where);

        if (logger.isDebugEnabled())
        {
            logger.debug("get ( {}, {}, {}, {} ) : {}", new Object[]{from, toType, throughType, Arrays.toString(fields), Arrays.toString(ts)});
        }
        return ts;
    }

    @Override
    public void put(RawEntity<?> from, RawEntity<?>[] through, Class<? extends RawEntity<?>> throughType, RawEntity<?>[] to, Class<? extends RawEntity<?>> toType, String[] fields, String where)
    {
        cache.put(from, through, throughType, to, toType, fields, where);

        if (logger.isDebugEnabled())
        {
            logger.debug("put ( {}, {}, {}, {}, {}, {}, {} )", new Object[]{from, Arrays.toString(through), throughType, Arrays.toString(to), toType, Arrays.toString(fields), where});
        }
    }

    @Override
    public void remove(Class<? extends RawEntity<?>>... types)
    {
        cache.remove(types);

        if (logger.isDebugEnabled())
        {
            logger.debug("remove ( {} )", Arrays.toString(types));
        }
    }

    @Override
    public void remove(RawEntity<?> entity, String[] fields)
    {
        cache.remove(entity, fields);

        if (logger.isDebugEnabled())
        {
            logger.debug("remove ( {}, {} )", entity, Arrays.toString(fields));
        }
    }

    @Override
    public void flush()
    {
        cache.flush();

        if (logger.isDebugEnabled())
        {
            logger.debug("flush ()");
        }
    }
}
