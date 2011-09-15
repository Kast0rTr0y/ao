package net.java.ao;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.java.ao.schema.FieldNameConverter;

/**
 * <p>Factory for building large number of {@link ReadOnlyEntityProxy} instances. Reflectively fetching information
 * about the implemented type is expensive and unnecessary to be done more than once.</p>
 * 
 * @author ahennecke
 *
 * @param <T> the entity type
 * @param <K> the primary key type
 */
public class ReadOnlyEntityProxyFactory<T extends RawEntity<K>, K>
{
    private final EntityManager entityManager;
    private final Class<T> type;
    
    private Set<Method> accessors;
    private Map<Method, String> fieldNames;

    @SuppressWarnings("rawtypes")
    public ReadOnlyEntityProxyFactory(EntityManager entityManager, Class<T> type)
    {
        this.entityManager = entityManager;
        this.type = type;
        
        // Cache information about the accessors (can be getters or annotated) and field names.
        // All instances built by this factory will make use of this information.
        accessors = new HashSet<Method>();
        fieldNames = new HashMap<Method, String>();
        FieldNameConverter fieldNameConverter = entityManager.getFieldNameConverter();

        // iterate over the class hierarchy and find the converted field names and accessors.
        // this is needed for the getter implementation of the proxy
        Class search = type;
        while (!Object.class.equals(search) && search != null) {
            for (Method method : search.getDeclaredMethods()) {
                if (Common.isAccessor(method)) {
                    fieldNames.put(method, fieldNameConverter.getName(method));
                    accessors.add(method);
                }
            }

            search = search.getSuperclass();
        }        
    }
    
    /**
     * @param primaryKey the primary key object
     * @return a new read only proxy instance, using cached class structure information
     */
    public ReadOnlyEntityProxy<T, K> build(K primaryKey) {
        return new ReadOnlyEntityProxy<T, K>(entityManager, type, primaryKey, fieldNames, accessors);
    }
}
