package net.java.ao.sql;

import org.junit.Test;

import java.util.regex.Matcher;

import static org.junit.Assert.*;

public class SqlUtilsTest
{
    @Test
    public void testOnClausePattern()
    {
        testOnClausePattern("id = otherId", null, "id", null, "otherId");
        testOnClausePattern("a.id = b.otherId", "a.", "id", "b.", "otherId");
    }

    private void testOnClausePattern(String onClause, String one, String two, String three, String four)
    {
        final Matcher matcher = SqlUtils.ON_CLAUSE.matcher(onClause);
        assertTrue(matcher.matches());
        assertEquals(one, matcher.group(1));
        assertEquals(two, matcher.group(2));
        assertEquals(three, matcher.group(3));
        assertEquals(four, matcher.group(4));
    }
}
