package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Test;

import static net.java.ao.schema.UnderscoreTableNameConverter.*;
import static org.junit.Assert.*;

public final class UnderscoreTableNameConverterTest
{
    @Test
    public void getLowerCaseNameForSimpleClassName()
    {
        testGetNameForSimpleClassName("simple_class_name", Case.LOWER);
    }

    @Test
    public void getUpperCaseNameForSimpleClassName()
    {
        testGetNameForSimpleClassName("SIMPLE_CLASS_NAME", Case.UPPER);
    }

    private void testGetNameForSimpleClassName(String expected, Case tableNameCase)
    {
        assertEquals(expected, new UnderscoreTableNameConverter(tableNameCase).getName(SimpleClassName.class));
    }

    private static interface SimpleClassName extends RawEntity<Object>
    {
    }
}
