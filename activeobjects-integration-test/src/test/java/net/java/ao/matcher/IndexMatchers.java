package net.java.ao.matcher;

import com.google.common.collect.ImmutableList;
import net.java.ao.schema.helper.Index;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.isEmptyOrNullString;

public class IndexMatchers {

    public static Matcher<Index> index(String indexName, String tableName, String fieldName) {

        return index(indexName, tableName, ImmutableList.of(fieldName));
    }

    public static Matcher<Index> index(String indexName, String tableName, List<String> fieldNames) {

        return Matchers.allOf(hasName(indexName), hasTable(tableName), hasFieldsInAnyOrder(fieldNames));
    }

    public static Matcher<Index> index(String tableName, String fieldName) {

        return index(tableName, ImmutableList.of(fieldName));
    }

    public static Matcher<Index> index(String tableName, List<String> fieldNames) {

        return Matchers.allOf(hasTable(tableName), hasFieldsInAnyOrder(fieldNames));
    }

    public static Matcher<Index> isNamed() {
        final Matcher<String> notEmptyString = CoreMatchers.not(isEmptyOrNullString());

        return new BaseMatcher<Index>() {
            @Override
            public boolean matches(Object o) {
                return notEmptyString.matches(o);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("indexName should be ");
                notEmptyString.describeTo(description);
            }

            @Override
            public void describeMismatch(Object o, Description description) {
                notEmptyString.describeMismatch(((Index) o).getIndexName(), description);
            }
        };
    }


    public static Matcher<Index> hasName(final String indexName) {
        return new BaseMatcher<Index>() {
            @Override
            public boolean matches(Object o) {
                final Index index = (Index) o;
                return Objects.equals(indexName, index.getIndexName());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("indexName should be ").appendValue(indexName);
            }

            @Override
            public void describeMismatch(Object o, Description description) {
                description.appendText("was ").appendValue(((Index) o).getIndexName());
            }
        };
    }

    public static Matcher<Index> hasTable(final String tableName) {
        return new BaseMatcher<Index>() {
            @Override
            public boolean matches(Object o) {
                final Index index = (Index) o;
                return Objects.equals(tableName, index.getTableName());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("tableName should be ").appendValue(tableName);
            }

            @Override
            public void describeMismatch(Object o, Description description) {
                description.appendText("was ").appendValue(((Index) o).getTableName());
            }
        };
    }

    public static Matcher<Index> hasFieldsInAnyOrder(final List<String> fieldNames) {
        final Matcher<Iterable<? extends String>> containsInAnyOrder = containsInAnyOrder(fieldNames.toArray(new String[fieldNames.size()]));

        return new BaseMatcher<Index>() {
            @Override
            public boolean matches(Object o) {
                final Index index = (Index) o;
                return containsInAnyOrder.matches(index.getFieldNames());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("fieldNames should be an ");
                containsInAnyOrder.describeTo(description);
            }

            @Override
            public void describeMismatch(Object o, Description description) {
                final Index index = (Index) o;
                containsInAnyOrder.describeMismatch(index.getFieldNames(), description);
            }
        };
    }
}
