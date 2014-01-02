package net.java.ao.db;

import net.java.ao.schema.ddl.SQLAction;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.mockito.internal.matchers.Matches;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Matcher for SQLAction statement.
 */
public class SqlActionStatementMatcher<T extends SQLAction> extends TypeSafeMatcher<T>
{
    public static Matcher<? super SQLAction> hasStatement(String statement)
    {
        //noinspection unchecked
        return new SqlActionStatementMatcher(equalTo(statement));
    }

    public static Matcher<? super SQLAction> hasStatementMatching(String regex)
    {
        //noinspection unchecked
        return new SqlActionStatementMatcher(new Matches(regex));
    }

    public static Matcher<? super SQLAction> hasStatement(Matcher<String> statementMatcher)
    {
        //noinspection unchecked
        return new SqlActionStatementMatcher(statementMatcher);
    }

    /**
     * Matcher used on the SQLAction's statement.
     */
    private final Matcher<String> matcher;

    /**
     * Creates a new matcher for an SQLAction's statement.
     *
     * @param statementMatcher a Matcher&lt;String&gt;
     */
    public SqlActionStatementMatcher(Matcher<String> statementMatcher)
    {
        this.matcher = statementMatcher;
    }

    @Override
    protected boolean matchesSafely(final T item)
    {
        return matcher.matches(item.getStatement());
    }

    @Override
    public void describeTo(final Description description)
    {
        description.appendText("a SQLAction with statement matching ");
        matcher.describeTo(description);
    }
}
