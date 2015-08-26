package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class CamelCaseTableNameConverterTest {
    private TableNameConverter converter = new CamelCaseTableNameConverter();

    @Test
    public void testGetNameForSimpleClassName() {
        assertEquals("simpleClassName", converter.getName(SimpleClassName.class));
    }

    @Test
    public void testGetNameForClassNameWithAcronym() {
        assertEquals("classNameWithACRONYM", converter.getName(ClassNameWithACRONYM.class));
    }

    private static interface SimpleClassName extends RawEntity<Object> {
    }

    private static interface ClassNameWithACRONYM extends RawEntity<Object> {
    }
}
