/*
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
package net.java.ao.db;

import net.java.ao.Disposable;
import net.java.ao.DisposableDataSource;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.reflect.Proxy.newProxyInstance;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * The NuoDB JDBC Driver supports ResultSet.TYPE_FORWARD_ONLY currently. The NuoDB data source handler intercepts
 * invocations of methods, which create SQL statements and forcibly overrides result set type to
 * {@link java.sql.ResultSet.TYPE_FORWARD_ONLY}. These are methods intercepted by the handler:
 * <ul>
 * <li>Statement createStatement(int resultSetType, int resultSetConcurrency);</li>
 * <li>PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency);</li>
 * <li>CallableStatement prepareCall(String sql, int resultSetType,  int resultSetConcurrency);</li>
 * </ul>
 *
 * @author Sergey Bushik
 */
public class NuoDBDisposableDataSourceHandler {

    /**
     * Creates reflection proxy for the specified NuoDB data source
     *
     * @param dataSource to intercept invocations
     * @return disposable data source
     */
    public static DisposableDataSource newInstance(DataSource dataSource) {
        return newInstance(dataSource, null);
    }

    public static DisposableDataSource newInstance(DataSource dataSource, Disposable disposable) {
        return (DisposableDataSource) newInstance(
                new Class[]{DisposableDataSource.class},
                new DelegatingDisposableDataSourceHandler(dataSource, disposable));
    }

    /**
     * Intercepts <tt>dispose()</tt> and <tt>getConnection()</tt> methods
     */
    static class DelegatingDisposableDataSourceHandler extends DelegatingInvocationHandler {

        private static final String GET_CONNECTION = "getConnection";
        private static final String DISPOSE = "dispose";

        private Disposable disposable;

        public DelegatingDisposableDataSourceHandler(DataSource dataSource, Disposable disposable) {
            super(dataSource);
            this.disposable = disposable;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object result = null;
            if (name.equals(DISPOSE) && parameterTypes.length == 0 && disposable != null) {
                disposable.dispose();
            } else if (name.equals(GET_CONNECTION)) {
                Connection connection = (Connection) delegate(method, args);
                result = newInstance(new Class[]{Connection.class}, new DelegatingConnectionHandler(connection));
            } else {
                result = delegate(method, args);
            }
            return result;
        }
    }

    /**
     * The handler, where result set type actually forcibly changed to those supported by NuoDB JDBC Driver.
     */
    static class DelegatingConnectionHandler extends DelegatingInvocationHandler {

        public static final String CREATE_STATEMENT = "createStatement";
        public static final String PREPARE_STATEMENT = "prepareStatement";
        public static final String PREPARE_CALL = "prepareCall";

        public DelegatingConnectionHandler(Connection connection) {
            super(connection);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (name.equals(CREATE_STATEMENT) && parameterTypes.length == 2) {
                // Statement createStatement(int resultSetType, int resultSetConcurrency);
                args[1] = TYPE_FORWARD_ONLY;
            } else if ((name.equals(PREPARE_STATEMENT) || name.equals(PREPARE_CALL)) && parameterTypes.length == 3) {
                // PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency);
                // CallableStatement prepareCall(String sql, int resultSetType,  int resultSetConcurrency);
                args[1] = TYPE_FORWARD_ONLY;
            }
            return super.invoke(proxy, method, args);
        }
    }

    private static Object newInstance(Class<?>[] interfaces, InvocationHandler invocationHandler) {
        return newProxyInstance(
                NuoDBDisposableDataSourceHandler.class.getClassLoader(),
                interfaces, invocationHandler);
    }

    /**
     * Base delegating invocation handler
     */
    static class DelegatingInvocationHandler implements InvocationHandler {

        private Object target;

        protected DelegatingInvocationHandler(Object target) {
            this.target = checkNotNull(target);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return delegate(method, args);
        }

        protected Object delegate(Method method, Object[] args) throws Throwable {
            final Method m = target.getClass().getMethod(method.getName(), method.getParameterTypes());
            m.setAccessible(true);
            try {
                return m.invoke(target, args);
            } catch (IllegalAccessException exception) {
                // avoid UndeclaredThrowableExceptions
                throw new RuntimeException(exception);
            } catch (IllegalArgumentException exception) {
                // avoid UndeclaredThrowableExceptions
                throw new RuntimeException(exception);
            } catch (InvocationTargetException exception) {
                // avoid UndeclaredThrowableExceptions
                throw exception.getCause();
            }
        }
    }
}
