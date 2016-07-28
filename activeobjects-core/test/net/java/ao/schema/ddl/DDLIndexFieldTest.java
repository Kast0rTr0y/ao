package net.java.ao.schema.ddl;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DDLIndexFieldTest {

    @Test
    public void shouldIgnoreNameCaseWhenComparing() {
        DDLIndexField field1 = DDLIndexField.builder().fieldName("field_name").build();
        DDLIndexField field2 = DDLIndexField.builder().fieldName("FIELD_NAME").build();

        assertThat(field1, equalTo(field2));
    }
}