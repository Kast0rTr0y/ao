package net.java.ao.event;

import java.util.List;

interface ListenerInvokerFactory
{
    List<ListenerInvoker> getInvokers(Object listener);
}
