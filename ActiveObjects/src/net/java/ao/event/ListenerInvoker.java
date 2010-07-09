package net.java.ao.event;

import java.util.Set;

interface ListenerInvoker
{
    Set<Class<?>> getSupportedEventTypes();

    void invoke(Object event);
}
