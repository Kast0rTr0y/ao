package net.java.ao.atlassian;

import net.java.ao.schema.DefaultIndexNameConverter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class AtlassianIndexNameConverterTest {

    private static final String REGULAR_TABLE_NAME = "table";
    private static final String SHORT_TABLE_NAME = "t";
    private static final String LONG_TABLE_NAME = "some_table_with_long_name_that_exceeds_30_characters";

    private static final String REGULAR_INDEX_NAME = "index";
    private static final String SHORT_INDEX_NAME = "i";
    private static final String LONG_INDEX_NAME = "some_index_with_long_name_that_exceeds_30_characters";


    @Parameterized.Parameters(name = "getName and getPrefix compatibility for table \"{0}\" and index \"{1}\"")
    public static Collection<Object[]> getTestParams() {
        return Arrays.asList(new Object[][] {
                { SHORT_TABLE_NAME, SHORT_INDEX_NAME },
                { SHORT_TABLE_NAME, REGULAR_INDEX_NAME },
                { SHORT_TABLE_NAME, LONG_INDEX_NAME },
                { REGULAR_TABLE_NAME, SHORT_INDEX_NAME },
                { REGULAR_TABLE_NAME, REGULAR_INDEX_NAME },
                { REGULAR_TABLE_NAME, LONG_INDEX_NAME },
                { LONG_TABLE_NAME, SHORT_INDEX_NAME },
                { LONG_TABLE_NAME, REGULAR_INDEX_NAME },
                { LONG_TABLE_NAME, LONG_INDEX_NAME }
        });
    }

    private final String tableName;
    private final String columnName;

    private final AtlassianIndexNameConverter sut;

    public AtlassianIndexNameConverterTest(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;

        sut = new AtlassianIndexNameConverter(new DefaultIndexNameConverter());
    }

    @Test
    public void shouldGetCompatibleShortenAndPrefix() {
        final String shortenedIndexName = sut.getName(tableName, columnName);
        final String indexNamePrefix = sut.getPrefix(tableName);

        System.out.println(shortenedIndexName);
        System.out.println(indexNamePrefix);

        assertThat(shortenedIndexName, startsWith(indexNamePrefix));
    }
}