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
package net.java.ao;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Daniel Spiewak
 */
class RelationsCache {
	private final Map<CacheKey, Entity[]> cache;
	private final Map<Class<? extends Entity>, Set<CacheKey>> typeMap;
	private final Map<MetaCacheKey, Set<CacheKey>> fieldMap;
	private final ReadWriteLock lock;

	public RelationsCache() {
		cache = new HashMap<CacheKey, Entity[]>();
		typeMap = new HashMap<Class<? extends Entity>, Set<CacheKey>>();
		fieldMap = new HashMap<MetaCacheKey, Set<CacheKey>>();
		
		lock = new ReentrantReadWriteLock();
	}

	public void put(Entity from, Entity[] to, String[] fields, Class<? extends Entity> throughType) {
		if (to.length == 0) {
			return;
		}
		
		CacheKey key = new CacheKey(from, to[0].getEntityType(), throughType, fields);
		lock.writeLock().lock();
		try {
			cache.put(key, to);
			
			Set<CacheKey> keys = typeMap.get(key.getThroughType());
			if (keys == null) {
				keys = new HashSet<CacheKey>();
				typeMap.put(key.getThroughType(), keys);
			}
			keys.add(key);
			
			for (String field : fields) {
				MetaCacheKey metaKey = new MetaCacheKey(from, key.getToType(), field);
				
				keys = fieldMap.get(metaKey);
				if (keys == null) {
					keys = new HashSet<CacheKey>();
					fieldMap.put(metaKey, keys);
				}
				keys.add(key);
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	public <T extends Entity> T[] get(Entity from, Class<T> toType, Class<? extends Entity> throughType, String[] fields) {
		lock.readLock().lock();
		try {
			return (T[]) cache.get(new CacheKey(from, toType, throughType, fields));
		} finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * The ReadWriteLock used internally to lock the caches.  This lock must be
	 * 	released manually using  {@link #unlock()}
	 */
	public void remove(Class<? extends Entity> type) {
		lock.writeLock().lock();
		try {
			Set<CacheKey> keys = typeMap.get(type);
			if (keys != null) {
				for (CacheKey key : keys) {
					cache.remove(key);
				}
	
				typeMap.remove(type);
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * The ReadWriteLock used internally to lock the caches.  This lock must be
	 * 	released manually using  {@link #unlock()}
	 */
	public void remove(Class<? extends Entity>[] types) {
		lock.writeLock().lock();
		try {
			for (Class<? extends Entity> type : types) {
				Set<CacheKey> keys = typeMap.get(type);
				if (keys != null) {
					for (CacheKey key : keys) {
						cache.remove(key);
					}
		
					typeMap.remove(type);
				}
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * The ReadWriteLock used internally to lock the caches.  This lock must be
	 * 	released manually using  {@link #unlock()}
	 */
	void remove(Entity from, Class<? extends Entity> toType, String[] fields) {
		lock.writeLock().tryLock();
		try {
			for (String field : fields) {
				Set<CacheKey> keys = fieldMap.get(new MetaCacheKey(from, toType, field));
				if (keys != null) {
					for (CacheKey key : keys) {
						cache.remove(key);
					}
				}
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	private static class CacheKey {
		private Entity from;
		private Class<? extends Entity> toType;
		private Class<? extends Entity> throughType;
		
		private String[] fields;
		
		public CacheKey(Entity from, Class<? extends Entity> toType, Class<? extends Entity> throughType, String[] fields) {
			this.from = from;
			this.toType = toType;
			this.throughType = throughType;
			
			setFields(fields);
		}

		public Entity getFrom() {
			return from;
		}

		public void setFrom(Entity from) {
			this.from = from;
		}

		public Class<? extends Entity> getToType() {
			return toType;
		}

		public void setToType(Class<? extends Entity> toType) {
			this.toType = toType;
		}

		public String[] getFields() {
			return fields;
		}

		public void setFields(String[] fields) {
			for (int i = 0; i < fields.length; i++) {
				fields[i] = fields[i].toLowerCase();
			}
			
			Arrays.sort(fields);
			
			this.fields = fields;
		}
		
		public Class<? extends Entity> getThroughType() {
			return throughType;
		}
		
		public void setThroughType(Class<? extends Entity> throughType) {
			this.throughType = throughType;
		}
		
		@Override
		public String toString() {
			return '(' + from.toString() + "; to=" + toType.getName() + "; through=" + throughType.getName() + "; " + Arrays.toString(fields) + ')';
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof CacheKey) {
				CacheKey key = (CacheKey) obj;
				
				if (key.getFrom() != null) {
					if (!key.getFrom().equals(from)) {
						return false;
					}
				}
				if (key.getToType() != null) {
					if (!key.getToType().getName().equals(toType.getName())) {
						return false;
					}
				}
				if (key.getThroughType() != null) {
					if (!key.getThroughType().getName().equals(throughType.getName())) {
						return false;
					}
				}
				if (key.getFields() != null) {
					if (!Arrays.equals(key.getFields(), fields)) {
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
			
			if (from != null) {
				hashCode += from.hashCode();
			}
			if (toType != null) {
				hashCode += toType.getName().hashCode();
			}
			if (throughType != null) {
				hashCode += throughType.getName().hashCode();
			}
			if (fields != null) {
				for (String field : fields) {
					hashCode += field.hashCode();
				}
			}
			
			return hashCode;
		}
	}
	
	private static class MetaCacheKey {
		private Entity from;
		private Class<? extends Entity> toType;
		private String field;
		
		public MetaCacheKey(Entity entity, Class<? extends Entity> toType, String field) {
			this.from = entity;
			this.toType = toType;
			
			setField(field);
		}

		public Entity getFrom() {
			return from;
		}

		public void setFrom(Entity entity) {
			this.from = entity;
		}

		public Class<? extends Entity> getToType() {
			return toType;
		}

		public void setToType(Class<? extends Entity> toType) {
			this.toType = toType;
		}

		public String getField() {
			return field;
		}

		public void setField(String field) {
			this.field = field.toLowerCase();
		}
		
		@Override
		public String toString() {
			return from.toString() + "; " + toType.getName() + "; " + field;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof MetaCacheKey) {
				MetaCacheKey key = (MetaCacheKey) obj;
				
				if (key.getFrom() != null) {
					if (!key.getFrom().equals(from)) {
						return false;
					}
				}
				if (key.getToType() != null) {
					if (!key.getToType().getName().equals(toType.getName())) {
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
			
			if (from != null) {
				hashCode += from.hashCode();
			}
			if (toType != null) {
				hashCode += toType.hashCode();
			}
			if (field != null) {
				hashCode += field.hashCode();
			}
			
			return hashCode;
		}
	}
}
