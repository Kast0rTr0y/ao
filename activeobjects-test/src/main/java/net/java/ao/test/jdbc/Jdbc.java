package net.java.ao.test.jdbc;

import net.java.ao.EntityManager;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Use with {@link ActiveObjectsJUnitRunner} to specify the JDBC configuration for the
 * {@link EntityManager} used in unit tests.  This can be either one of the
 * database-specific implementations of {@link JdbcConfiguration}, such as {@link Postgres},
 * or {@link DynamicJdbcConfiguration}. The default is {@link Hsql}.
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface Jdbc {
    Class<? extends JdbcConfiguration> value() default DynamicJdbcConfiguration.class;
}
