package net.java.ao.schema;

import net.java.ao.Accessor;
import net.java.ao.ManyToMany;
import net.java.ao.Mutator;
import net.java.ao.OneToMany;
import net.java.ao.OneToOne;
import net.java.ao.RawEntity;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

public final class AbstractFieldNameConverterTest
{
    private static final String NAME_OF_ANNOTATED_METHOD = "nameOfAnnotatedMethod";

    private AbstractFieldNameConverterForTests converter;

    @Before
    public void setUp()
    {
        converter = new AbstractFieldNameConverterForTests();
    }

    @Test
    public void getNameForMethodAnnotatedWithOneToOne() throws Exception
    {
        assertNull(converter.getName(method("oneToOne")));
    }

    @Test
    public void getNameForMethodAnnotatedWithOneToMany() throws Exception
    {
        assertNull(converter.getName(method("oneToMany")));
    }
    
    @Test
    public void getNameForMethodAnnotatedWithManyToMany() throws Exception
    {
        assertNull(converter.getName(method("manyToMany")));
    }

    @Test
    public void getNameForMethodAnnotatedWithMutator() throws Exception
    {
        assertEquals(NAME_OF_ANNOTATED_METHOD, converter.getName(method("changeThis", SomeInterface.class)));
    }

    @Test
    public void getNameForMethodAnnotatedWithAccessor() throws Exception
    {
        assertEquals(NAME_OF_ANNOTATED_METHOD, converter.getName(method("accessThis")));
    }
    
    @Test
    public void getNameForSetterMethodWithEntity() throws Exception
    {
        assertEquals("ThisID", converter.getName(method("setThis", SomeInterface.class)));
    }

    @Test
    public void getNameForIsMethod() throws Exception
    {
        assertEquals("ThisID", converter.getName(method("isThis")));
    }

    @Test
    public void getNameForGetterMethod() throws Exception
    {
        assertEquals("ThisID", converter.getName(method("getThis")));
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

        public SomeInterface isThis()
        {
            return null;
        }

        public void setThis(SomeInterface si)
        {
        }

        @Accessor(NAME_OF_ANNOTATED_METHOD)
        public SomeInterface accessThis()
        {
            return null;
        }

        @Mutator(NAME_OF_ANNOTATED_METHOD)
        public void changeThis(SomeInterface si)
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

    private static final class AbstractFieldNameConverterForTests extends AbstractFieldNameConverter
    {
        @Override
        public String convertName(String name)
        {
            return name;
        }
    }
}
