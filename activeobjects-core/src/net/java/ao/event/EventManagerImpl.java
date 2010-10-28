package net.java.ao.event;

/**
 * <p>The default implementation of the {@link net.java.ao.event.EventManager}</p>
 * <p>The implementation can be configured by specifying an alternative implementation
 * of the {@link net.java.ao.event.ListenerInvokerFactory}</p>
 * <p>By default it uses a {@link net.java.ao.event.SingleParameterAnnotatedMethodListenerInvokerFactory}</p>
 */
public class EventManagerImpl implements EventManager
{
    private final Lisneters listeners = new Lisneters();
    private final ListenerInvokerFactory invokerFactory;

    public EventManagerImpl()
    {
        this(new SingleParameterAnnotatedMethodListenerInvokerFactory());
    }

    public EventManagerImpl(ListenerInvokerFactory invokerFactory)
    {
        this.invokerFactory = invokerFactory;
    }

    public void publish(Object event)
    {
        for (Class<?> eventType : ClassUtils.findAllTypes(event.getClass()))
        {
            for (ListenerInvoker invoker : listeners.get(eventType).invokers())
            {
                invoker.invoke(event);
            }
        }
    }

    public void register(Object listener)
    {
        listeners.register(listener, invokerFactory.getInvokers(listener));
    }

    public void unregister(Object listener)
    {
        listeners.remove(listener);
    }
}
