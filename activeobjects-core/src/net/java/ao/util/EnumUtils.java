package net.java.ao.util;

import com.google.common.collect.Iterables;

import java.lang.reflect.InvocationTargetException;

import static com.google.common.collect.Lists.newArrayList;

public final class EnumUtils
{
    public static Iterable<Enum> values(Class<? extends Enum> type)
    {
        try
        {
            return newArrayList((Enum[]) type.getMethod("values").invoke(null));
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalStateException(e);
        }
        catch (SecurityException e)
        {
            throw new IllegalStateException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new IllegalStateException(e);
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public static int size(Class<? extends Enum> type)
    {
        return Iterables.size(values(type));
    }
}
