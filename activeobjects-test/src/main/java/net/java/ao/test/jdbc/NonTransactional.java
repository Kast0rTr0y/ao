package net.java.ao.test.jdbc;

import net.java.ao.test.junit.ActiveObjectsJUnitRunner;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * An annotation that disables the default transaction management behavior for JUnit test methods.
 * <p>
 * {@link ActiveObjectsJUnitRunner} normally surrounds each test method execution in a transaction,
 * which is then rolled back so that the next test starts with a clean database state.  This will
 * not work if the test does its own transaction management and commits changes.  In that case,
 * mark the test method with @NonTransactional; this makes the test runner not create a transaction,
 * but instead completely drop and recreate the database after executing the test.
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
public @interface NonTransactional {
}
