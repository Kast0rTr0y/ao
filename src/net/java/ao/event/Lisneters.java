package net.java.ao.event;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class Lisneters
{
    private final ConcurrentMap<Class<?>, Invokers> invokers = new MapMaker().makeComputingMap(new Function<Class<?>, Invokers>()
    {
        public Invokers apply(final Class<?> from)
        {
            return new Invokers();
        }
    });

    void register(final Object listener, final Iterable<ListenerInvoker> invokers)
    {
        for (final ListenerInvoker invoker : invokers)
        {
            register(listener, invoker);
        }
    }

    private void register(final Object listener, final ListenerInvoker invoker)
    {
        for (final Class<?> eventClass : invoker.getSupportedEventTypes())
        {
            invokers.get(eventClass).add(listener, invoker);
        }
    }


    void remove(final Object listener)
    {
        for (final Invokers entry : invokers.values())
        {
            entry.remove(listener);
        }
    }

    public Invokers get(Class<?> eventType)
    {
        return invokers.get(eventType);
    }

    static final class Invokers
    {
        private final ConcurrentMap<Object, ListenerInvoker> invokers = new ConcurrentHashMap<Object, ListenerInvoker>();

        public void add(Object listener, ListenerInvoker invoker)
        {
            invokers.put(listener, invoker);
        }

        public void remove(Object listener)
        {
            invokers.remove(listener);
        }

        public Collection<ListenerInvoker> invokers()
        {
            return invokers.values();
        }
    }
}
