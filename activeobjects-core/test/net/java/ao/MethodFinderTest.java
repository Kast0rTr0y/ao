package net.java.ao;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public final class MethodFinderTest
{
    private MethodFinder methodFinder;

    @Before
    public void setUp()
    {
        methodFinder = new MethodFinder();
    }

    @Test
    public void testFindAnnotatedMethodsWithNoAnnotatedMethods()
    {
        final Iterable<Method> methods = methodFinder.findAnnotatedMethods(AnAnnotation.class, Object.class);
        assertEquals(0, Iterables.size(methods));
    }

    @Test
    public void testFindAnnotatedMethodsWithAnnotatedMethods()
    {
        final Iterable<Method> methods = methodFinder.findAnnotatedMethods(AnAnnotation.class, AnnotatedClass.class);
        assertEquals(2, Iterables.size(methods));
        assertContainsMethodNamed(methods, "annotatedMethod");
        assertContainsMethodNamed(methods, "anotherAnnotatedMethod");
    }

    private void assertContainsMethodNamed(Iterable<Method> methods, final String name)
    {
        assertTrue(Iterables.any(methods, new Predicate<Method>()
        {
            @Override
            public boolean apply(Method m)
            {
                return m.getName().equals(name);
            }
        }));
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface AnAnnotation
    {
    }

    public static final class AnnotatedClass
    {
        @AnAnnotation
        public void annotatedMethod()
        {
        }

        public void notAnnotatedMethod()
        {
        }

        @AnAnnotation
        public void anotherAnnotatedMethod()
        {
        }
    }
}
