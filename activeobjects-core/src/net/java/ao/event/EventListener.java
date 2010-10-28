package net.java.ao.event;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <p>Interface to annotate methods of event listeners.</p>
 * <p>Annotated methods must have one and only one parameter, the type of which defines
 * handled events.</p>
 */
@Retention(RUNTIME)
@Target(METHOD)
@Documented
public @interface EventListener
{
}
