package net.java.ao.schema.ddl;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;


public class DDLIndexTest {

    @Test
    public void shouldIgnoreTableNameCaseWhenComparing() {
        DDLIndex index1 = DDLIndex.builder().table("table_name").build();
        DDLIndex index2 = DDLIndex.builder().table("TABLE_NAME").build();

        assertThat(index1, equalTo(index2));
    }

    @Test
    public void shouldIgnoreIndexNameWhenComparing() {
        DDLIndex index1 = DDLIndex.builder().table("table_name").build();
        DDLIndex index2 = DDLIndex.builder().indexName("some_name").table("table_name").build();

        assertThat(index1, equalTo(index2));
    }

    @Test
    public void shouldCheckFieldsWhenComparing() {

        DDLIndexField field1 = DDLIndexField.builder().fieldName("field1").build();
        DDLIndexField field2 = DDLIndexField.builder().fieldName("field2").build();

        DDLIndex index1 = DDLIndex.builder().table("table_name").fields(field1, field2).build();
        DDLIndex index2 = DDLIndex.builder().table("table_name").fields(field1, field2).build();

        assertThat(index1, equalTo(index2));
    }

    @Test
    public void shouldCheckFieldsOrderWhenComparing() {

        DDLIndexField field1 = DDLIndexField.builder().fieldName("field1").build();
        DDLIndexField field2 = DDLIndexField.builder().fieldName("field2").build();

        DDLIndex index1 = DDLIndex.builder().table("table_name").fields(field1, field2).build();
        DDLIndex index2 = DDLIndex.builder().table("table_name").fields(field2, field1).build();

        assertThat(index1, not(equalTo(index2)));
    }
}