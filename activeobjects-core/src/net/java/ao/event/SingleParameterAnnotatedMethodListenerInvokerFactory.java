package net.java.ao.event;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

class SingleParameterAnnotatedMethodListenerInvokerFactory implements ListenerInvokerFactory
{
    private static final Class ANNOTATION = EventListener.class;

    public List<ListenerInvoker> getInvokers(final Object listener)
    {
        final List<Method> methods = getValidMethods(listener);

        if (methods.isEmpty())
        {
            throw new RuntimeException("No valid method found for listener of type " + listener.getClass().getName());
        }

        return Lists.transform(methods, new Function<Method, ListenerInvoker>()
        {
            public ListenerInvoker apply(Method method)
            {
                return new SingleParameterMethodListenerInvoker(listener, method);
            }
        });
    }

    private List<Method> getValidMethods(Object listener)
    {
        final List<Method> annotatedMethods = Lists.newArrayList();
        for (Method method : listener.getClass().getMethods())
        {
            if (isValidMethod(method))
            {
                annotatedMethods.add(method);
            }
        }
        return annotatedMethods;
    }

    private boolean isValidMethod(Method method)
    {
        if (isAnnotated(method))
        {
            if (hasOneAndOnlyOneParameter(method))
            {
                return true;
            }
            else
            {
                throw new RuntimeException("Method <" + method + "> of class <" + method.getDeclaringClass() + "> " +
                        "is annotated with <" + ANNOTATION.getClass().getName() + "> but has 0 or more than 1 parameters! " +
                        "Listener methods MUST have 1 and only 1 parameter.");
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean isAnnotated(Method method)
    {
        return method.getAnnotation(ANNOTATION) != null;
    }

    private boolean hasOneAndOnlyOneParameter(Method method)
    {
        return method.getParameterTypes().length == 1;
    }

    private static class SingleParameterMethodListenerInvoker implements ListenerInvoker
    {
        private final Object listener;
        private final Method method;

        public SingleParameterMethodListenerInvoker(Object listener, Method method)
        {
            this.listener = listener;
            this.method = method;
        }

        public Set<Class<?>> getSupportedEventTypes()
        {
            return Sets.newHashSet(method.getParameterTypes());
        }

        public void invoke(Object event)
        {
            try
            {
                method.invoke(listener, event);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e)
            {
                if (e.getCause() == null)
                {
                    throw new RuntimeException(e);
                }
                else if (e.getCause().getMessage() == null)
                {
                    throw new RuntimeException(e.getCause());
                }
                else
                {
                    throw new RuntimeException(e.getCause().getMessage(), e.getCause());
                }
            }
        }
    }
}
