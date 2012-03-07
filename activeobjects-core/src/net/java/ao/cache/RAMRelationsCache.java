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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.java.ao.RawEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Daniel Spiewak
 */
public final class RAMRelationsCache implements RelationsCache {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final Map<RAMRelationsCacheKey, RawEntity<?>[]> cache;
	private final Map<Class<? extends RawEntity<?>>, Set<RAMRelationsCacheKey>> typeMap;
	private final Map<MetaCacheKey, Set<RAMRelationsCacheKey>> fieldMap;
	private final ReadWriteLock lock;

	public RAMRelationsCache() {
		cache = new HashMap<RAMRelationsCacheKey, RawEntity<?>[]>();
		typeMap = new HashMap<Class<? extends RawEntity<?>>, Set<RAMRelationsCacheKey>>();
		fieldMap = new HashMap<MetaCacheKey, Set<RAMRelationsCacheKey>>();
		
		lock = new ReentrantReadWriteLock();
	}
	
	public void flush() {
		lock.writeLock().lock();
		try {
			cache.clear();
			typeMap.clear();
			fieldMap.clear();
            logger.debug("flush");
		} finally {
			lock.writeLock().unlock();
		}
	}

	public void put(RawEntity<?> from, RawEntity<?>[] through, Class<? extends RawEntity<?>> throughType, RawEntity<?>[] to, Class<? extends RawEntity<?>> toType, String[] fields, String where) {

		final RAMRelationsCacheKey key = new RAMRelationsCacheKey(from, toType, throughType, fields, where);
		lock.writeLock().lock();
		try {
			cache.put(key, to);
			
			Set<RAMRelationsCacheKey> keys = typeMap.get(key.getThroughType());
			if (keys == null) {
				keys = new HashSet<RAMRelationsCacheKey>();
				typeMap.put(key.getThroughType(), keys);
			}
			keys.add(key);
			
			for (String field : fields) {
				for (RawEntity<?> entity : through) {
					MetaCacheKey metaKey = new MetaCacheKey(entity, field);
					
					keys = fieldMap.get(metaKey);
					if (keys == null) {
						keys = new HashSet<RAMRelationsCacheKey>();
						fieldMap.put(metaKey, keys);
					}
					keys.add(key);
				}
			}
            if (logger.isDebugEnabled())
            {
                logger.debug("put ( {}, {}, {}, {}, {}, {} )", new Object[]{from, through, throughType, to, toType, Arrays.toString(fields)});
            }
        } finally {
			lock.writeLock().unlock();
		}
	}

	public <T extends RawEntity<K>, K> T[] get(RawEntity<?> from, Class<T> toType, Class<? extends RawEntity<?>> throughType, String[] fields, String where) {
		lock.readLock().lock();
		try {
            final T[] ts = (T[]) cache.get(new RAMRelationsCacheKey(from, toType, throughType, fields, where));
            if (logger.isDebugEnabled())
            {
                logger.debug("get ( {}, {}, {}, {} ) : {}", new Object[]{from, toType, throughType, Arrays.toString(fields), Arrays.toString(ts)});
            }
            return ts;
		} finally {
			lock.readLock().unlock();
		}
	}
	
	public void remove(Class<? extends RawEntity<?>>... types) {
		lock.writeLock().lock();
		try {
			for (Class<? extends RawEntity<?>> type : types) {
				Set<RAMRelationsCacheKey> keys = typeMap.get(type);
				if (keys != null) {
					for (RAMRelationsCacheKey key : keys) {
						cache.remove(key);
					}
		
					typeMap.remove(type);
				}
			}
            if (logger.isDebugEnabled())
            {
                logger.debug("remove ( {} )", Arrays.toString(types));
            }
        } finally {
			lock.writeLock().unlock();
		}
	}

	public void remove(RawEntity<?> entity, String[] fields) {
		lock.writeLock().lock();
		try {
			for (String field : fields) {
				Set<RAMRelationsCacheKey> keys = fieldMap.get(new MetaCacheKey(entity, field));
				if (keys != null) {
					for (RAMRelationsCacheKey key : keys) {
						cache.remove(key);
					}
				}
			}
            if (logger.isDebugEnabled())
            {
                logger.debug("remove ( {}, {} )", entity, Arrays.toString(fields));
            }
        } finally {
			lock.writeLock().unlock();
		}
	}

    private static final class MetaCacheKey {
		private final RawEntity<?> entity;
		private final String field;
		
		public MetaCacheKey(RawEntity<?> entity, String field) {
			this.entity = entity;
            this.field = field;
        }

		public RawEntity<?> getEntity() {
			return entity;
		}

		public String getField() {
			return field;
		}

		@Override
		public String toString() {
			return entity.toString() + "; " + field;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof MetaCacheKey) {
				MetaCacheKey key = (MetaCacheKey) obj;
				
				if (key.getEntity() != null) {
					if (!key.getEntity().equals(entity)) {
						return false;
					}
				}
				if (key.getField() != null) {
					if (!key.getField().equals(field)) {
						return false;
					}
				}
				
				return true;
			}
			
			return super.equals(obj);
		}
		
		@Override
		public int hashCode() {
			int hashCode = 0;
			
			if (entity != null) {
				hashCode += entity.hashCode();
			}
			if (field != null) {
				hashCode += field.hashCode();
			}
			hashCode %= 2 << 15;
			
			return hashCode;
		}
	}
}
