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
    
    private Set<Method> accessors = new HashSet<Method>();;
    private Map<Method, String> fieldNames = new HashMap<Method, String>();
    private Map<String, String> polymorphicFieldNames = new HashMap<String, String>();
    private Map<String, Class<?>> returnTypes = new HashMap<String, Class<?>>();
    
    /**
     * Cache information about the accessors (can be getters or annotated) and field names.
     * All instances built by this factory will make use of this information.
     */
    public ReadOnlyEntityProxyFactory(EntityManager entityManager, Class<T> type)
    {
        this.entityManager = entityManager;
        this.type = type;
        
        FieldNameConverter fieldNameConverter = entityManager.getFieldNameConverter();

        // iterate over the class hierarchy and find the converted field names and accessors.
        // this is needed for the getter implementation of the proxy as well as reading/converting the data values
        
        // go through the current interface and all superinterfaces to collect accessor information
        Set<Class<?>> types = new HashSet<Class<?>>();
        readTypeHierarchy(types, type);
        
        for (Class<?> search : types)
        {
            for (Method method : search.getDeclaredMethods()) {
                if (Common.isAccessor(method)) {
                    String fieldName = fieldNameConverter.getName(method);
                    if (fieldName != null) {
                        fieldNames.put(method, fieldName);
                        accessors.add(method);
    
                        // figure out if there's a polymorphic annotation and keep track of the respective field name
                        Class<?> attributeType = Common.getAttributeTypeFromMethod(method);
                        if (attributeType != null) {
                            String polyFieldName = (attributeType.getAnnotation(Polymorphic.class) == null ? null : 
                                entityManager.getFieldNameConverter().getPolyTypeName(method));
                            
                            polymorphicFieldNames.put(fieldName, polyFieldName);
                        }
                        
                        // keep track of the return types, so we can use the db field types to convert the values
                        returnTypes.put(fieldName, method.getReturnType());
                    }
                }
            }
        }        
        
    }

    /**
     * Recursively read the interface hierarchy of the given AO type interface
     */
    private void readTypeHierarchy(Set<Class<?>> types, Class<?> type)
    {
        types.add(type);
        for (Class<?> superType : type.getInterfaces())
        {
            readTypeHierarchy(types, superType);
        }
    }
    
    /**
     * @param primaryKey the primary key object
     * @return a new read only proxy instance, using cached class structure information
     */
    public ReadOnlyEntityProxy<T, K> build(K primaryKey) {
        return new ReadOnlyEntityProxy<T, K>(entityManager, type, primaryKey, fieldNames, polymorphicFieldNames, returnTypes, accessors);
    }
}
