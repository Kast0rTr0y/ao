package net.java.ao.event;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * A utility class for the small event system.
 */
final class ClassUtils
{
    private ClassUtils()
    {
    }

    /**
     * Finds all super classes and interfaces for a given class
     *
     * @param cls The class to scan
     * @return The collected related classes found
     */
    static Set<Class<?>> findAllTypes(final Class<?> cls)
    {
        final Set<Class<?>> types = Sets.newHashSet();
        findAllTypes(cls, types);
        return types;
    }

    /**
     * Finds all super classes and interfaces for a given class
     *
     * @param cls   The class to scan
     * @param types The collected related classes found
     */
    private static void findAllTypes(final Class<?> cls, final Set<Class<?>> types)
    {
        if (cls == null)
        {
            return;
        }

        // check to ensure it hasn't been scanned yet
        if (types.contains(cls))
        {
            return;
        }

        types.add(cls);

        findAllTypes(cls.getSuperclass(), types);
        for (int x = 0; x < cls.getInterfaces().length; x++)
        {
            findAllTypes(cls.getInterfaces()[x], types);
        }
    }
}
