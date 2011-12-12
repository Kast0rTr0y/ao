package net.java.ao.atlassian;

import net.java.ao.ActiveObjectsException;
import net.java.ao.RawEntity;
import net.java.ao.schema.Table;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class AtlassianTableNameConverterTest
{
    private static final String PREFIX = "PFX";

    private AtlassianTableNameConverter converter;

    @Before
    public void setUp() throws Exception
    {
        converter = new AtlassianTableNameConverter(mockPrefix());
    }

    @Test
    public void testGetNameForSimpleEntity() throws Exception
    {
        assertEquals(PREFIX + "_" + "SIMPLE_ENTITY", converter.getName(SimpleEntity.class));
    }

    @Test
    public void testGetNameForAnnotatedEntity() throws Exception
    {
        assertEquals(PREFIX + "_" + "SIMPLE_ENTITY", converter.getName(AnnotatedEntity.class));
    }

    @Test(expected = ActiveObjectsException.class)
    public void testGetNameForEntityWithTooLongName() throws Exception
    {
        converter.getName(EntityWithAVeryVeryLongName.class);
    }

    private static interface SimpleEntity extends RawEntity<Object>
    {
    }

    @Table("SimpleEntity")
    private static interface AnnotatedEntity extends RawEntity<Object>
    {
    }

    private static interface EntityWithAVeryVeryLongName extends RawEntity<Object>
    {
    }

    private static TablePrefix mockPrefix()
    {
        return new TablePrefix()
        {
            @Override
            public String prepend(String string)
            {
                return PREFIX + "_" + string;
            }
        };
    }
}
