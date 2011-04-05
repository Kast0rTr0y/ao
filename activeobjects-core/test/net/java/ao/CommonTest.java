package net.java.ao;

import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

public final class CommonTest
{
    @Test
    public void testGetAttributeTypeFromGetterMethod() throws Exception
    {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("getThis")));
    }

    @Test
    public void testGetAttributeTypeFromMethodInvalidGetterMethod() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("getInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromIsMethod() throws Exception
    {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("isThis")));
    }

    @Test
    public void testGetAttributeTypeFromMethodInvalidIsMethod() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("isInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromSetterMethod() throws Exception
    {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("setThis", SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromInvalidSetterMethodWithNoArguments() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("setInvalid0")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidSetterMethodWithMoreThanOneArgument() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("setInvalid2", SomeInterface.class, SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithAccessor() throws Exception
    {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("accessThis")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithAccessor() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("accessInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithMutator() throws Exception
    {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("changeThis", SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithMutatorAndNoArguments() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("changeInvalid0")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithMutatorAndMoreThanOneArguments() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("changeInvalid2", SomeInterface.class, SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithOneToOne() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("oneToOne")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithOneToMany() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("oneToMany")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithManyToMany() throws Exception
    {
        assertNull(Common.getAttributeTypeFromMethod(method("manyToMany")));
    }

    private static Method method(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException
    {
        return ClassWithMethods.class.getMethod(methodName, parameterTypes);
    }

    private static final class ClassWithMethods
    {
        public SomeInterface getThis()
        {
            return null;
        }

        public void getInvalid()
        {
        }

        public SomeInterface isThis()
        {
            return null;
        }

        public void isInvalid()
        {
        }

        public void setThis(SomeInterface si)
        {
        }

        public void setInvalid0()
        {
        }

        public void setInvalid2(SomeInterface si1, SomeInterface si2)
        {
        }

        @Accessor("")
        public SomeInterface accessThis()
        {
            return null;
        }

        @Accessor("")
        public void accessInvalid()
        {
        }

        @Mutator("")
        public void changeThis(SomeInterface si)
        {
        }

        @Mutator("")
        public void changeInvalid0()
        {
        }

        @Mutator("")
        public void changeInvalid2(SomeInterface si1, SomeInterface si2)
        {
        }

        @OneToOne
        public void oneToOne()
        {
        }

        @OneToMany
        public void oneToMany()
        {
        }

        @ManyToMany(value = SomeInterface.class, where = "")
        public void manyToMany()
        {
        }
    }

    private static interface SomeInterface extends RawEntity<Object>
    {
    }
}
