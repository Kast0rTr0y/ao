package net.java.ao.schema;

import net.java.ao.RawEntity;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public final class TableAnnotationTableNameConverterTest {
    private TableNameConverter tableAnnotationTableNameConverter;

    @Mock
    private TableNameConverter delegateTableNameConverter;

    @Before
    public void setUp() {
        tableAnnotationTableNameConverter = new TableAnnotationTableNameConverter(delegateTableNameConverter);
    }

    @Test
    public void getNameForClassWithTableAnnotationAndAValidAnnotationValue() throws Exception {
        assertEquals(EntityWithTableAnnotationAndValidAnnotationValue.VALID_TABLE_NAME,
                tableAnnotationTableNameConverter.getName(EntityWithTableAnnotationAndValidAnnotationValue.class));
    }

    @Test(expected = IllegalStateException.class)
    public void getNameForClassWithTableAnnotationAndEmptyAnnotationValue() throws Exception {
        tableAnnotationTableNameConverter.getName(EntityWithTableAnnotationAndEmptyAnnotationValue.class);
    }

    @Test
    public void getNameForClassWithNoTableAnnotation() throws Exception {
        tableAnnotationTableNameConverter.getName(EntityWithNoTableAnnotation.class);
        verify(delegateTableNameConverter).getName(EntityWithNoTableAnnotation.class);
    }

    @Table(EntityWithTableAnnotationAndValidAnnotationValue.VALID_TABLE_NAME)
    private static interface EntityWithTableAnnotationAndValidAnnotationValue extends RawEntity<Object> {
        String VALID_TABLE_NAME = "ValidTableName";
    }

    @Table("")
    private static interface EntityWithTableAnnotationAndEmptyAnnotationValue extends RawEntity<Object> {
    }

    private static interface EntityWithNoTableAnnotation extends RawEntity<Object> {
    }
}
