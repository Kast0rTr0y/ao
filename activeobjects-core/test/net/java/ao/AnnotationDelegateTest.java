package net.java.ao;

import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

public final class AnnotationDelegateTest
{
    @Test(expected = NullPointerException.class)
    public void firstMethodArgumentNullThrowsNullPointerException() throws Exception
    {
        new AnnotationDelegate(null, nonAnnotatedMethod());
    }

    @Test
    public void secondMethodArgumentNullDoesNotThrowNullPointerException() throws Exception
    {
        new AnnotationDelegate(nonAnnotatedMethod(), null);
    }

    @Test
    public void getAnnotationReturnsNullWhenAnnotationIsNotPresent() throws Exception
    {
        assertNull(new AnnotationDelegate(nonAnnotatedMethod(), nonAnnotatedMethod()).getAnnotation(AnAnnotation.class));
        assertNull(new AnnotationDelegate(nonAnnotatedMethod(), null).getAnnotation(AnAnnotation.class));
    }

    @Test
    public void isAnnotationPresentReturnsFalseWhenAnnotationIsNotPresent() throws Exception
    {
        assertFalse(new AnnotationDelegate(nonAnnotatedMethod(), nonAnnotatedMethod()).isAnnotationPresent(AnAnnotation.class));
        assertFalse(new AnnotationDelegate(nonAnnotatedMethod(), null).isAnnotationPresent(AnAnnotation.class));
    }
    
    @Test
    public void getAnnotationReturnsNonNullWhenAnnotationIsPresent() throws Exception
    {
        assertNotNull(new AnnotationDelegate(annotatedMethod(), nonAnnotatedMethod()).getAnnotation(AnAnnotation.class));
        assertNotNull(new AnnotationDelegate(nonAnnotatedMethod(), annotatedMethod()).getAnnotation(AnAnnotation.class));
        assertNotNull(new AnnotationDelegate(annotatedMethod(), annotatedMethod()).getAnnotation(AnAnnotation.class));
        assertNotNull(new AnnotationDelegate(annotatedMethod(), null).getAnnotation(AnAnnotation.class));
    }

    @Test
    public void isAnnotationPresentReturnsFalseWhenAnnotationIsPresent() throws Exception
    {
        assertTrue(new AnnotationDelegate(annotatedMethod(), nonAnnotatedMethod()).isAnnotationPresent(AnAnnotation.class));
        assertTrue(new AnnotationDelegate(nonAnnotatedMethod(), annotatedMethod()).isAnnotationPresent(AnAnnotation.class));
        assertTrue(new AnnotationDelegate(annotatedMethod(), annotatedMethod()).isAnnotationPresent(AnAnnotation.class));
        assertTrue(new AnnotationDelegate(annotatedMethod(), null).isAnnotationPresent(AnAnnotation.class));
    }

    private static Method nonAnnotatedMethod() throws NoSuchMethodException
    {
        return method("voidMethod");
    }

    private static Method annotatedMethod() throws NoSuchMethodException
    {
        return method("annotatedMethod");
    }

    private static Method method(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException
    {
        return SomeInterface.class.getMethod(methodName, parameterTypes);
    }

    private static interface SomeInterface extends RawEntity<Object>
    {
        void voidMethod();

        @AnAnnotation
        void annotatedMethod();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    private static @interface AnAnnotation
    {
    }
}
