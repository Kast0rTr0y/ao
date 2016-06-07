package net.java.ao;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.info.EntityInfo;
import net.java.ao.schema.info.FieldInfo;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CommonTest {
    @Test
    public void testGetAttributeTypeFromGetterMethod() throws Exception {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("getThis")));
    }

    @Test
    public void testGetAttributeTypeFromMethodInvalidGetterMethod() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("getInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromIsMethod() throws Exception {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("isThis")));
    }

    @Test
    public void testGetAttributeTypeFromMethodInvalidIsMethod() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("isInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromSetterMethod() throws Exception {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("setThis", SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromInvalidSetterMethodWithNoArguments() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("setInvalid0")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidSetterMethodWithMoreThanOneArgument() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("setInvalid2", SomeInterface.class, SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithAccessor() throws Exception {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("accessThis")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithAccessor() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("accessInvalid")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithMutator() throws Exception {
        assertEquals(SomeInterface.class, Common.getAttributeTypeFromMethod(method("changeThis", SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithMutatorAndNoArguments() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("changeInvalid0")));
    }

    @Test
    public void testGetAttributeTypeFromInvalidMethodAnnotatedWithMutatorAndMoreThanOneArguments() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("changeInvalid2", SomeInterface.class, SomeInterface.class)));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithOneToOne() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("oneToOne")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithOneToMany() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("oneToMany")));
    }

    @Test
    public void testGetAttributeTypeFromMethodAnnotatedWithManyToMany() throws Exception {
        assertNull(Common.getAttributeTypeFromMethod(method("manyToMany")));
    }

    @Test
    public void testGetValueFieldNames() {
        final Set<FieldInfo> fields = new HashSet<FieldInfo>();

        final FieldInfo valueField = mock(FieldInfo.class);
        when(valueField.getJavaType()).thenReturn(String.class);
        final Method valueMethod = String.class.getMethods()[0];
        when(valueField.hasAccessor()).thenReturn(true);
        when(valueField.getAccessor()).thenReturn(valueMethod);
        fields.add(valueField);

        final FieldInfo writeOnlyField = mock(FieldInfo.class);
        when(writeOnlyField.getJavaType()).thenReturn(String.class);
        when(writeOnlyField.hasAccessor()).thenReturn(false);
        final Method writeOnlyMethod = String.class.getMethods()[2];
        when(writeOnlyField.getAccessor()).thenReturn(writeOnlyMethod);
        fields.add(writeOnlyField);

        final FieldInfo referenceField = mock(FieldInfo.class);
        when(referenceField.getJavaType()).thenReturn(Entity.class);
        final Method referenceMethod = String.class.getMethods()[1];
        when(referenceField.hasAccessor()).thenReturn(true);
        when(referenceField.getAccessor()).thenReturn(referenceMethod);
        fields.add(referenceField);

        final EntityInfo entityInfo = mock(EntityInfo.class);
        when(entityInfo.getFields()).thenReturn(fields);

        final FieldNameConverter fieldNameConverter = mock(FieldNameConverter.class);
        when(fieldNameConverter.getName(valueMethod)).thenReturn("valueMethod");
        when(fieldNameConverter.getName(writeOnlyMethod)).thenReturn("writeOnlyMethod");
        when(fieldNameConverter.getName(referenceMethod)).thenReturn("referenceMethod");

        final Set<String> valueFieldsNames = Common.getValueFieldsNames(entityInfo, fieldNameConverter);
        assertThat(valueFieldsNames, Matchers.contains("valueMethod"));
    }

    @Test
    public final void testShorten() {
        testShorten("a-very-long-string");
        testShorten("another-very-long-string");
        testShorten("yet-another-very-long-string");
    }

    private void testShorten(String s) {
        for (int i = 0; i < s.length() * 2; i++) {
            assertShortenWithShortString(s, i);
        }
        for (int i = 1; i < s.length() / 2 + 1; i++) {
            assertShortenWithLongerString(s, i);
        }
    }

    private void assertShortenWithLongerString(String s, int minusMaxLength) {
        final int maxLength = s.length() - minusMaxLength;
        final String shortened = Common.shorten(s, maxLength);

        assertTrue(shortened.length() <= maxLength);
        assertEquals(shortened, Common.shorten(s, maxLength));
    }

    private void assertShortenWithShortString(String s, int plusMaxLength) {
        assertEquals(s, Common.shorten(s, s.length() + plusMaxLength));
    }


    private static Method method(String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
        return InterfaceWithMethods.class.getMethod(methodName, parameterTypes);
    }

    @SuppressWarnings("unused")
    private static interface InterfaceWithMethods extends RawEntity<Object> {
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

    private static interface SomeInterface extends RawEntity<Object> {
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

    private static interface OtherInterface extends RawEntity<Object> {
    }
}
