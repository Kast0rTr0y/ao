package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public final class CachingTableNameConverterTest
{
    private TableNameConverter cachingTableNameConverter;

    @Mock
    private TableNameConverter delegateTableNameConverter;

    @Before
    public void setUp()
    {
        when(delegateTableNameConverter.getName(anyRawEntityClass())).thenReturn("table_name");
        cachingTableNameConverter = new CachingTableNameConverter(delegateTableNameConverter);
    }

    @Test
    public void getNameIsCachedWhenCalledMoreThanOnce()
    {
        final String name = cachingTableNameConverter.getName(SomeEntity.class);
        assertEquals(name, cachingTableNameConverter.getName(SomeEntity.class));
        verify(delegateTableNameConverter, times(1)).getName(SomeEntity.class);
    }

    private static interface SomeEntity extends RawEntity<Object>
    {

    }

    private static Class<? extends RawEntity<?>> anyRawEntityClass()
    {
        return Matchers.anyObject();
    }
}
