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
final class RAMCacheLayer implements CacheLayer {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Map<String, Object> values;
	private final ReadWriteLock valueLock = new ReentrantReadWriteLock();

	private Set<String> dirty;
	private final ReadWriteLock dirtyLock = new ReentrantReadWriteLock();

	private Set<Class<? extends RawEntity<?>>> flush;
	private final ReadWriteLock flushLock = new ReentrantReadWriteLock();

	RAMCacheLayer() {
		values = new HashMap<String, Object>();
		dirty = new HashSet<String>();
		flush = new HashSet<Class<? extends RawEntity<?>>>();
	}

	public void clear() {
		dirtyLock.readLock().lock();
		valueLock.writeLock().lock();
		try {
			Set<String> toRemove = new HashSet<String>();
			for (String field : values.keySet()) {
				if (!dirty.contains(field)) {
					toRemove.add(field);
				}
			}

			for (String field : toRemove) {
				values.remove(field);
			}
            logger.debug("clear");
		} finally {
			valueLock.writeLock().unlock();
			dirtyLock.readLock().unlock();
		}
	}

	public void clearDirty() {
		dirtyLock.writeLock().lock();
		try {
			dirty.clear();
            logger.debug("clearDirty");
		} finally {
			dirtyLock.writeLock().unlock();
		}

	}

	public void clearFlush() {
		flushLock.writeLock().lock();
		try {
			flush.clear();
            logger.debug("clearFlush");
		} finally {
			flushLock.writeLock().unlock();
		}
	}

	public boolean contains(String field)
    {
		valueLock.readLock().lock();
		try {
            final boolean contains = values.containsKey(field);
            logger.debug("contains ( {} ) : {}", field, contains);
            return contains;
		} finally {
			valueLock.readLock().unlock();
		}
	}

	public boolean dirtyContains(String field) {
		dirtyLock.readLock().lock();
		try {
            final boolean contains = dirty.contains(field);
            logger.debug("dirtyContains ( {} ) : {}", field, contains);
            return contains;
		} finally {
			dirtyLock.readLock().unlock();
		}
	}

	public Object get(String field) {
		valueLock.readLock().lock();
		try {
            final Object o = values.get(field);
            logger.debug("get ( {} ) : {}", field, o);
            return o;
		} finally {
			valueLock.readLock().unlock();
		}
	}

	public String[] getDirtyFields() {
		dirtyLock.readLock().lock();
		try {
            final String[] strings = dirty.toArray(new String[dirty.size()]);
            logger.debug("getDirtyFields : {}", Arrays.toString(strings));
            return strings;
		} finally {
			dirtyLock.readLock().unlock();
		}
	}

	public Class<? extends RawEntity<?>>[] getToFlush() {
		flushLock.readLock().lock();
		try {
            final Class[] classes = flush.toArray(new Class[flush.size()]);
            logger.debug("getToFlush : {}", classes);
            return classes;
		} finally {
			flushLock.readLock().unlock();
		}
	}

	public void markDirty(String field) {
		dirtyLock.writeLock().lock();
		try {
			dirty.add(field);
            logger.debug("markDirty( {} )", field);
		} finally {
			dirtyLock.writeLock().unlock();
		}
	}

	public void markToFlush(Class<? extends RawEntity<?>> type) {
		flushLock.writeLock().lock();
		try {
			flush.add(type);
            logger.debug("markToFlush ( {} )", type);
		} finally {
			flushLock.writeLock().unlock();
		}
	}

	public void put(String field, Object value) {
		valueLock.writeLock().lock();
		try {
			values.put(field, value);
            logger.debug("put ( {}, {} )", field, value);
		} finally {
			valueLock.writeLock().unlock();
		}
	}

	public void remove(String field) {
		valueLock.writeLock().lock();
		try {
			values.remove(field);
            logger.debug("remove ( {} )", field);
		} finally {
			valueLock.writeLock().unlock();
		}
	}
}
