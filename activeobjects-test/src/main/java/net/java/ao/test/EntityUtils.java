package net.java.ao.test;

import com.google.common.base.Predicate;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;

import java.lang.reflect.Method;

import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Lists.newArrayList;

public final class EntityUtils {
    public static String getTableName(EntityManager em, Class<? extends RawEntity<?>> entityType) {
        return getTableName(em, entityType, true);
    }

    /**
     * Get the table name of the given class entity
     *
     * @param entityType the class of the entity
     * @param escape     whether or not to escape the table name
     * @return the table name
     */
    public static String getTableName(EntityManager em, Class<? extends RawEntity<?>> entityType, boolean escape) {
        final String tableName = em.getProvider().shorten(em.getTableNameConverter().getName(entityType));
        return escape ? em.getProvider().withSchema(tableName) : tableName;
    }

    public static String getFieldName(EntityManager em, Class<? extends RawEntity<?>> entityType, String methodName) {
        return em.getProvider().shorten(em.getFieldNameConverter().getName(findMethod(entityType, methodName)));
    }

    public static String getPolyFieldName(EntityManager em, Class<? extends RawEntity<?>> entityType, String methodName) {
        return em.getProvider().shorten(em.getFieldNameConverter().getPolyTypeName(findMethod(entityType, methodName)));
    }

    public static String escapeFieldName(EntityManager em, Class<? extends RawEntity<?>> entityType, String methodName) {
        return escapeKeyword(em, getFieldName(em, entityType, methodName));
    }

    public static String escapePolyFieldName(EntityManager em, Class<? extends RawEntity<?>> entityType, String methodName) {
        return escapeKeyword(em, getPolyFieldName(em, entityType, methodName));
    }


    public static String escapeKeyword(EntityManager em, String keyword) {
        return em.getProvider().processID(keyword);
    }

    private static Method findMethod(Class<? extends RawEntity<?>> entityType, final String methodName) {
        return find(newArrayList(entityType.getMethods()), new Predicate<Method>() {
            @Override
            public boolean apply(Method m) {
                return m.getName().equals(methodName);
            }
        });
    }
}
