package net.java.ao;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class CommonShortenPrefixTest {

    @Parameterized.Parameters(name = "Shorten \"{0}\" to {1} characters")
    public static Collection<Object[]> getTestParams() {
        return Arrays.asList(new Object[][] {
                { "short", 30 },
                { "19_char_long_name__", 30 },
                { "20_char_long_name___", 30 },
                { "ao_000000_table_some_generic_index_name", 30 },
                { "ao_000000_table_some_generic_index_name_which_is_very_very_long", 30 },
        });
    }

    private final String indexName;
    private final int maxIndexLength;

    public CommonShortenPrefixTest(String indexName, int maxIndexLength) {
        this.indexName = indexName;
        this.maxIndexLength = maxIndexLength;
    }

    @Test
    public void shouldGetCompatibleShortenAndPrefix() {
        final String shortenedIndexName = Common.shorten(indexName, maxIndexLength);
        final String indexNamePrefix = Common.prefix(indexName, maxIndexLength);

        assertThat(shortenedIndexName, startsWith(indexNamePrefix));
    }
}
