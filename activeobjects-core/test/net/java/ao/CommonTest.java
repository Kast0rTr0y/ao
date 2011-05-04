package net.java.ao;

import net.java.ao.schema.AbstractFieldNameConverter;
import net.java.ao.schema.PrimaryKey;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Set;

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

    @Test
    public void testGetValueFieldNames()
    {
        final Set<String> valueFieldsNames = Common.getValueFieldsNames(SomeInterface.class, new TestFieldNameConverter());
        assertEquals(2, valueFieldsNames.size());
        assertTrue(valueFieldsNames.contains("PrimaryKey"));
        assertTrue(valueFieldsNames.contains("Field1"));
        assertFalse(valueFieldsNames.contains("Relation1"));
        assertFalse(valueFieldsNames.contains("Relation2"));
        assertFalse(valueFieldsNames.contains("Relation3"));
    }

    private static Method method(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException
    {
        return InterfaceWithMethods.class.getMethod(methodName, parameterTypes);
    }

    @SuppressWarnings("unused")
    private static interface InterfaceWithMethods extends RawEntity<Object>
    {
        public SomeInterface getThis();

        public void getInvalid();

        public SomeInterface isThis();

        public void isInvalid();

        public void setThis(SomeInterface si);

        public void setInvalid0();

        public void setInvalid2(SomeInterface si1, SomeInterface si2);

        @Accessor("")
        public SomeInterface accessThis();

        @Accessor("")
        public void accessInvalid();

        @Mutator("")
        public void changeThis(SomeInterface si);

        @Mutator("")
        public void changeInvalid0();

        @Mutator("")
        public void changeInvalid2(SomeInterface si1, SomeInterface si2);

        @OneToOne
        public void oneToOne();

        @OneToMany
        public void oneToMany();

        @ManyToMany(value = SomeInterface.class, where = "")
        public void manyToMany();
    }

    private static interface SomeInterface extends RawEntity<Object>
    {
        public void setField1(String field1);

        public String getField1();

        @PrimaryKey
        public String getPrimaryKey();

        public void setPrimaryKey(String pk);

        @OneToOne
        public OtherInterface getRelation1();

        @OneToMany
        public OtherInterface[] getRelation2();

        @ManyToMany(value = OtherInterface.class)
        public OtherInterface[] getRelation3();
    }

    private static interface OtherInterface extends RawEntity<Object>
    {}

    private static final class TestFieldNameConverter extends AbstractFieldNameConverter
    {
        @Override
        protected String convertName(String name)
        {
            return name;
        }
    }
}
