package net.java.ao.builder;

import net.java.ao.Disposable;
import net.java.ao.DisposableDataSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static com.google.common.base.Preconditions.*;

public final class DelegatingDisposableDataSourceHandler implements InvocationHandler
{
    private final DataSource dataSource;
    private final Disposable disposable;

    public DelegatingDisposableDataSourceHandler(DataSource dataSource, Disposable disposable)
    {
        this.dataSource = checkNotNull(dataSource);
        this.disposable = checkNotNull(disposable);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        if (isDisposeMethod(method))
        {
            disposable.dispose();
            return null;
        }

        return delegate(method, args);
    }

    private Object delegate(Method method, Object[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        final Method m = dataSource.getClass().getMethod(method.getName(), method.getParameterTypes());
        m.setAccessible(true);
        return m.invoke(dataSource, args);
    }

    public static DisposableDataSource newInstance(DataSource ds, Disposable disposable)
    {
        return (DisposableDataSource) Proxy.newProxyInstance(
                DelegatingDisposableDataSourceHandler.class.getClassLoader(),
                new Class[]{DisposableDataSource.class},
                new DelegatingDisposableDataSourceHandler(ds, disposable));
    }

    private static boolean isDisposeMethod(Method method)
    {
        return method.getName().equals("dispose")
                && method.getParameterTypes().length == 0;
    }
}
