package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Test;

import static org.junit.Assert.*;

public final class UnderscoreTableNameConverterTest
{
    @Test
    public void getLowerCaseNameForSimpleClassName()
    {
        testGetNameForSimpleClassName("simple_class_name1", Case.LOWER);
    }

    @Test
    public void getUpperCaseNameForSimpleClassName()
    {
        testGetNameForSimpleClassName("SIMPLE_CLASS_NAME1", Case.UPPER);
    }

    private void testGetNameForSimpleClassName(String expected, Case tableNameCase)
    {
        assertEquals(expected, new UnderscoreTableNameConverter(tableNameCase).getName(SimpleClassName1.class));
    }

    private static interface SimpleClassName1 extends RawEntity<Object>
    {
    }
}
