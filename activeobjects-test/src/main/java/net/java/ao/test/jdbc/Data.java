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
 * Use with {@link ActiveObjectsJUnitRunner} to specify schema initialization behavior.
 * <p>
 * The class specified by this annotation must implement {@link DatabaseUpdater}, and must have a
 * no-args constructor.  The test runner will instantiate this class and call its
 * {@link DatabaseUpdater#update(net.java.ao.EntityManager)} method whenever the database
 * is to be reinitialized (once at the beginning of each test class that uses a unique
 * database configuration, and once after each test method that is marked with {@link NonTransactional @NonTransactional}).
 * Normally the {@link DatabaseUpdater#update(net.java.ao.EntityManager)} method should
 * just call {@link EntityManager#migrate(Class...)} with a list of entity classes; it can
 * also add any data that should exist before running tests.
 */
@Documented
@Retention(RUNTIME)
@Target(TYPE)
@Inherited
public @interface Data {
    Class<? extends DatabaseUpdater> value() default EmptyDatabase.class;
}
