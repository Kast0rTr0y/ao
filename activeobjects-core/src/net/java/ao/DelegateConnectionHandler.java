/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.*;

final class DelegateConnectionHandler implements InvocationHandler
{
    private final Connection delegate;
    private boolean closeable;

    private DelegateConnectionHandler(Connection delegate)
    {
        this.delegate = checkNotNull(delegate);
        this.closeable = true;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        if (isSetCloseableMethod(method))
        {
            setCloseable((Boolean) args[0]);
            return Void.TYPE;
        }

        if (isIsCloseableMethod(method))
        {
            return isCloseable();
        }


        if (isCloseMethod(method))
        {
            close();
            return Void.TYPE;
        }


        else if (isIsClosedMethod(method))
        {
            return isClosed();
        }

        return delegate(method, args);
    }

    private void setCloseable(boolean closeable)
    {
        this.closeable = closeable;
    }

    private boolean isCloseable()
    {
        return closeable;
    }

    private void close() throws SQLException
    {
        if (isCloseable())
        {
            delegate.close();
        }
    }

    private boolean isClosed() throws SQLException
    {
        return delegate.isClosed();
    }

    private Object delegate(Method method, Object[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException
    {
        final Method m = delegate.getClass().getMethod(method.getName(), method.getParameterTypes());
        m.setAccessible(true);
        return m.invoke(delegate, args);
    }

    public static DelegateConnection newInstance(Connection c)
    {
        return (DelegateConnection) Proxy.newProxyInstance(
                DelegateConnectionHandler.class.getClassLoader(),
                new Class[]{DelegateConnection.class},
                new DelegateConnectionHandler(c));
    }

    private static boolean isSetCloseableMethod(Method method)
    {
        return method.getName().equals("setCloseable")
                && method.getParameterTypes().length == 1
                && method.getParameterTypes()[0].equals(boolean.class);
    }


    private static boolean isIsCloseableMethod(Method method)
    {
        return method.getName().equals("isCloseable")
                && method.getParameterTypes().length == 0;
    }

    private static boolean isCloseMethod(Method method)
    {
        return method.getName().equals("close")
                && method.getParameterTypes().length == 0;
    }

    private static boolean isIsClosedMethod(Method method)
    {
        return method.getName().equals("isClosed")
                && method.getParameterTypes().length == 0;
    }
}
