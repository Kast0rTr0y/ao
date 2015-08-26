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
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.reflect.Proxy.newProxyInstance;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Types.BIGINT;
import static org.slf4j.LoggerFactory.getLogger;

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

    private static final ClassLoader CLASS_LOADER = NuoDBDisposableDataSourceHandler.class.getClassLoader();

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
        return (DisposableDataSource) newProxy(
                new Class[]{DisposableDataSource.class},
                new DelegatingDisposableDataSourceHandler(dataSource, disposable));
    }

    private static Object newProxy(Class<?>[] interfaces, InvocationHandler invocationHandler) {
        Object proxy = newProxyInstance(CLASS_LOADER, interfaces, invocationHandler);
        return proxy;
    }

    /**
     * Intercepts <tt>dispose()</tt> and <tt>getConnection()</tt> methods
     */
    static class DelegatingDisposableDataSourceHandler extends DelegatingInvocationHandler<DataSource> {

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
            // call dispose
            if (name.equals(DISPOSE) && parameterTypes.length == 0 && disposable != null) {
                disposable.dispose();
            // proxy getConnection() invocation
            } else if (name.equals(GET_CONNECTION)) {
                Connection connection = (Connection) delegate(method, args);
                result = newProxy(new Class[]{Connection.class},
                        new DelegatingConnectionHandler(connection));
            } else {
                result = delegate(method, args);
            }
            return result;
        }
    }

    /**
     * The handler, where result set type actually forcibly changed to those supported by NuoDB JDBC Driver.
     */
    static class DelegatingConnectionHandler extends DelegatingInvocationHandler<Connection> {

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
            Class<? extends Statement> statement = null;
            Integer resultSetTypeArgIndex = null;
            // change result set type to java.sql.Types.TYPE_FORWARD_ONLY
            if (name.equals(CREATE_STATEMENT)) {
                statement = Statement.class;
                // Statement createStatement(int resultSetType, int resultSetConcurrency);
                // Statement createStatement(int resultSetType, int resultSetConcurrency,
                //    int resultSetHoldability) throws SQLException;
                if (parameterTypes.length >= 2) {
                    resultSetTypeArgIndex = 0;
                }
            } else if (name.equals(PREPARE_STATEMENT)) {
                // PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency);
                // PreparedStatement prepareStatement(String sql, int resultSetType,
                //    int resultSetConcurrency, int resultSetHoldability);
                statement = PreparedStatement.class;
                if (parameterTypes.length >= 3) {
                    resultSetTypeArgIndex = 1;
                }
            } else if (name.equals(PREPARE_CALL)) {
                statement = CallableStatement.class;
                // CallableStatement prepareCall(String sql, int resultSetType,  int resultSetConcurrency);
                // CallableStatement prepareCall(String sql, int resultSetType,
                //    int resultSetConcurrency, int resultSetHoldability) throws SQLException;
                if (parameterTypes.length >= 3) {
                    resultSetTypeArgIndex = 1;
                }
            }
            if (resultSetTypeArgIndex != null &&
                    ((Integer) args[resultSetTypeArgIndex]) != TYPE_FORWARD_ONLY) {
                args[resultSetTypeArgIndex] = TYPE_FORWARD_ONLY;
                if (logger.isTraceEnabled()) {
                    logger.trace("Result set type changed to forward only");
                }
            }
            Object result = super.invoke(proxy, method, args);
            return statement != null ? newProxy(new Class[]{statement},
                    new DelegatingStatementHandler((Connection) proxy, (Statement) result)) : result;
        }
    }

    static class DelegatingStatementHandler extends DelegatingInvocationHandler<Statement> {

        private static final String GET_CONNECTION = "getConnection";
        private static final String GET_RESULT_SET = "getResultSet";
        private static final String EXECUTE_QUERY = "executeQuery";
        private Connection connection;

        public DelegatingStatementHandler(Connection connection, Statement statement) {
            super(statement);
            this.connection = connection;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            Object result;
            // proxy connection
            if (name.equals(GET_CONNECTION)) {
                result = connection;
            // proxy result set
            } else if (name.equals(EXECUTE_QUERY) || name.equals(GET_RESULT_SET)) {
                result = newProxy(new Class[]{ResultSet.class},
                        new DelegatingResultSetHandler((Statement) proxy,
                                (ResultSet) super.invoke(proxy, method, args)));
            } else {
                result = super.invoke(proxy, method, args);
            }
            return result;
        }
    }

    static class DelegatingResultSetHandler extends DelegatingInvocationHandler<ResultSet> {

        private static final String GET_STATEMENT = "getStatement";
        private static final String GET_OBJECT = "getObject";
        private ResultSetMetaData metaData;
        private Statement statement;
        private Map<String, Integer> columns;

        protected DelegatingResultSetHandler(Statement statement, ResultSet resultSet) throws SQLException {
            super(resultSet);
            this.statement = statement;
            this.metaData = target.getMetaData();
            int count = metaData.getColumnCount();
            Map<String, Integer> columns = newHashMap();
            for (int index = 0; index < count; index++) {
                int column = index + 1;
                columns.put(metaData.getColumnName(column), column);
            }
            this.columns = columns;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            Object result;
            // proxy statement
            if (name.equals(GET_STATEMENT)) {
                result = statement;
            // change resultSet.getObject() for BIGINT to return resultSet.getLong()
            } else if (name.equals(GET_OBJECT)) {
                Integer column = null;
                if (method.getParameterTypes()[0].equals(String.class)) {
                    column = columns.get(args[0]);
                } else {
                    column = (Integer) args[0];
                }
                // fixes get object for java.sql.Types.BIGINT to return long
                if (column != null && metaData.getColumnType(column) == BIGINT) {
                    result = target.getLong(column);
                } else {
                    result = super.invoke(proxy, method, args);
                }
            } else {
                result = super.invoke(proxy, method, args);
            }
            return result;
        }
    }

    /**
     * Base delegating invocation handler
     */
    static class DelegatingInvocationHandler<T> implements InvocationHandler {

        protected final Logger logger = getLogger(getClass());
        protected final T target;

        protected DelegatingInvocationHandler(T target) {
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
