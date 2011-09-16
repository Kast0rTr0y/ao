package net.java.ao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import net.java.ao.types.DatabaseType;
import net.java.ao.types.TypeManager;

/**
 * <p>A read-only representation of a database row proxy, mapped to the specified Entity type.</p>
 * 
 * <p>This implementation is used for streaming and intended to be as lightweight as possible. It only supports getters
 * or designated {@link Accessor} methods. Calling setters or save will result in an exception. Other method calls will 
 * be ignored.</p> 
 * 
 * <p>As much as possible, reflection calls should be kept out of this implementation. If there is information needed about
 * the implemented type, please use {@link ReadOnlyEntityProxyFactory} to cache them.</p>
 * 
 * <p>TODO There is currently some overlap with {@link EntityProxy}. As soon as this is battle-hardened, these can be refactored
 * into an abstract superclass, for example.</p>
 * 
 * @author ahennecke
 *
 * @param <T> the type of Entity
 * @param <K> the primary key type
 */
public class ReadOnlyEntityProxy<T extends RawEntity<K>, K> implements InvocationHandler
{
    private final K key;
    
    private final Class<T> type;

    private final EntityManager manager;
    
    private ImplementationWrapper<T> implementation;
    private final Map<String, Object> values = new HashMap<String, Object>();

    private final Map<Method, String> fieldNames;
    private final Map<String, String> polymorphicFieldNames;
    private final Set<Method> accessors;
    private final Map<String, Class<?>> returnTypes;

    public ReadOnlyEntityProxy(EntityManager manager, Class<T> type, K key, Map<Method, String> fieldNames, Map<String, String> polymorphicFieldNames, Map<String, Class<?>> returnTypes, Set<Method> accessors) {
        this.key = key;
        this.type = type;
        this.manager = manager;
        this.fieldNames = fieldNames;
        this.polymorphicFieldNames = polymorphicFieldNames;
        this.returnTypes = returnTypes;
        this.accessors = accessors;
    }

    @SuppressWarnings("unchecked")
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // optimise reflection call
        String methodName = method.getName();
        
        if(methodName.equals("getEntityProxy")) {
            return this;
        } else if (methodName.equals("getEntityType")) {
            return type;
        } else if (methodName.equals("save")) {
            // someone tried to call "save" at a read-only instance. We don't simply ignore that but rather throw an
            // exception, so the client knows that the call did not do what he expected.
            throw new RuntimeException("'save' method called on a read-only entity of type " + type.getSimpleName());
        } 

        if (implementation == null) {
            implementation = new ImplementationWrapper<T>();
            implementation.init((T) proxy);
        }

        MethodImplWrapper methodImpl = implementation.getMethod(methodName, method.getParameterTypes());
        if (methodImpl != null) {
            final Class<?> declaringClass = methodImpl.getMethod().getDeclaringClass();
            if (!Object.class.equals(declaringClass)) {
                // We don't want to return get the class Class using Class.forName as this doesn't play well
                // with multiple ClassLoaders (if the AO library has a separate class loader to the AO client)
                // Instead just compare the classNames
                final String callingClassName = Common.getCallingClassName(1);
                if (callingClassName == null || !callingClassName.equals(declaringClass.getName())) {
                    return methodImpl.getMethod().invoke(methodImpl.getInstance(), args);
                }
            }
        }

        if (methodName.equals("getEntityManager")) {
            return getManager();
        } else if (methodName.equals("hashCode")) {
            return hashCodeImpl();
        } else if (methodName.equals("equals")) {
            return equalsImpl((RawEntity<K>) proxy, args[0]);
        } else if (methodName.equals("toString")) {
            return toStringImpl();
        } else if (methodName.equals("init")) {
            return null;
        }
        
        Accessor accessorAnnotation = method.getAnnotation(Accessor.class);

        // check annotations first, they trump all
        if (accessorAnnotation != null) {
            return invokeGetter((RawEntity<?>) proxy, getKey(), accessorAnnotation.value(), method.getReturnType());
            
        } else if (accessors.contains(method)) {
            return invokeGetter((RawEntity<?>) proxy, getKey(), fieldNames.get(method), method.getReturnType());
            
        } else if (Common.isMutator(method) || method.isAnnotationPresent(Mutator.class)) {
            // someone tried to call "save" at a read-only instance. We don't simply ignore that but rather throw an
            // exception, so the client knows that the call did not do what he expected.
            throw new RuntimeException("Setter method called on a read-only entity of type " + type.getSimpleName() + ": " + methodName);
        }
        
        return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addValue(String fieldName, ResultSet res) throws SQLException {
        Class type = returnTypes.get(fieldName);
        String polyName = polymorphicFieldNames.get(fieldName);
        
        Object value = convertValue(res, fieldName, polyName, type);
        values.put(fieldName, value);
    }

    public K getKey() {
        return key;
    }

    public int hashCodeImpl() {
        return (key.hashCode() + type.hashCode()) % (2 << 15);
    }

    public boolean equalsImpl(RawEntity<K> proxy, Object obj) {
        if (proxy == obj) {
            return true;
        }

        if (obj instanceof RawEntity<?>) {
            RawEntity<?> entity = (RawEntity<?>) obj;
            
            String ourTableName = getManager().getTableNameConverter().getName(proxy.getEntityType());
            String theirTableName = getManager().getTableNameConverter().getName(entity.getEntityType());

            return Common.getPrimaryKeyValue(entity).equals(key) && theirTableName.equals(ourTableName);
        }

        return false;
    }

    public String toStringImpl() {
        String pkFieldName = Common.getPrimaryKeyField(type, getManager().getFieldNameConverter());
        return getManager().getTableNameConverter().getName(type) + " {" + pkFieldName + " = " + key.toString() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof ReadOnlyEntityProxy<?, ?>) {
            ReadOnlyEntityProxy<?, ?> proxy = (ReadOnlyEntityProxy<?, ?>) obj;

            if (proxy.type.equals(type) && proxy.key.equals(key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashCodeImpl();
    }

    Class<T> getType() {
        return type;
    }

    private EntityManager getManager() {
        return manager;
    }

    @SuppressWarnings("unchecked")
    private <V> V invokeGetter(RawEntity<?> entity, K key, String name, Class<V> type) throws Throwable {
        Object value = values.get(name);

        if (instanceOf(value, type)) {
            return handleNullReturn((V) value, type);
        } else if (isBigDecimal(value, type)) { // Oracle for example returns BigDecimal when we expect doubles
            return (V) handleBigDecimal(value, type);
        } 
        
        return handleNullReturn(null, type);
    }
    
    private <V> V convertValue(ResultSet res, String field, String polyName, Class<V> type) throws SQLException
    {
        if (isNull(res, field))
        {
            return null;
        }
        
        if (polyName != null) {
            Class<? extends RawEntity<?>> entityType = (Class<? extends RawEntity<?>>) type;
            entityType = getManager().getPolymorphicTypeMapper().invert(entityType, res.getString(polyName));
            
            type = (Class<V>) entityType;       // avoiding Java cast oddities with generics
        }
        
        TypeManager manager = TypeManager.getInstance();
        DatabaseType<V> databaseType = manager.getType(type);
        
        if (databaseType == null) {
            throw new RuntimeException("UnrecognizedType: " + type.toString());
        }
        
        return databaseType.pullFromDatabase(getManager(), res, type, field);
    }
    
    private boolean isNull(ResultSet res, String field) throws SQLException
    {
        res.getObject(field);
        return res.wasNull();
    }

    @SuppressWarnings("unchecked")
    private <V> V handleNullReturn(V back, Class<V> type) {
        if (back != null) {
            return back;
        }
        
        if (type.isPrimitive()) {
            if (type.equals(boolean.class)) {
                return (V) new Boolean(false);
            } else if (type.equals(char.class)) {
                return (V) new Character(' ');
            } else if (type.equals(int.class)) {
                return (V) new Integer(0);
            } else if (type.equals(short.class)) {
                return (V) new Short("0");
            } else if (type.equals(long.class)) {
                return (V) new Long("0");
            } else if (type.equals(float.class)) {
                return (V) new Float("0");
            } else if (type.equals(double.class)) {
                return (V) new Double("0");
            } else if (type.equals(byte.class)) {
                return (V) new Byte("0");
            }
        }

        return null;
    }

    private boolean instanceOf(Object value, Class<?> type) {
        if (value == null) {
            return true;
        }
        
        if (type.isPrimitive()) {
            if (type.equals(boolean.class)) {
                return instanceOf(value, Boolean.class);
            } else if (type.equals(char.class)) {
                return instanceOf(value, Character.class);
            } else if (type.equals(byte.class)) {
                return instanceOf(value, Byte.class);
            } else if (type.equals(short.class)) {
                return instanceOf(value, Short.class);
            } else if (type.equals(int.class)) {
                return instanceOf(value, Integer.class);
            } else if (type.equals(long.class)) {
                return instanceOf(value, Long.class);
            } else if (type.equals(float.class)) {
                return instanceOf(value, Float.class);
            } else if (type.equals(double.class)) {
                return instanceOf(value, Double.class);
            }
        } else {
            return type.isInstance(value);
        }

        return false;
    }

    /**
     * Some DB (Oracle) return BigDecimal for about any number
     */
    private boolean isBigDecimal(Object value, Class<?> type)
    {
        if (!(value instanceof BigDecimal))
        {
            return false;
        }
        return type.equals(int.class) || type.equals(long.class) || type.equals(float.class) || type.equals(double.class);
    }

    private Object handleBigDecimal(Object value, Class<?> type)
    {
        final BigDecimal bd = (BigDecimal) value;
        if (type.equals(int.class))
        {
            return bd.intValue();
        }
        else if (type.equals(long.class))
        {
            return bd.longValue();
        }
        else if (type.equals(float.class))
        {
            return bd.floatValue();
        }
        else if (type.equals(double.class))
        {
            return bd.doubleValue();
        }
        else
        {
            throw new RuntimeException("Could not resolve actual type for object :" + value + ", expected type is " + type);
        }
    }

}
