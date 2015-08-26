package net.java.ao.sql;

import com.google.common.base.Function;
import org.junit.Test;

import java.util.regex.Matcher;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class SqlUtilsTest {
    private static final TestIdProcessor TEST_ID_PROCESSOR = new TestIdProcessor();
    private static final String[] SEPARATORS = new String[]{"=", "!=", "<=", ">=", "<", ">", "<>", "like", "LIKE", "is", "IS", "IS NOT", "is not"};

    @Test
    public void testWhereClause() {
        for (String s : SEPARATORS) {
            testWhereClauseWithSeparator(s);
        }
    }

    private void testWhereClauseWithSeparator(String s) {
        testWhereClause(format("field %s value", s), "field");
        testWhereClause(format("field %s ?", s), "field");

        testWhereClause(format("field1 %s value1 AND field2 %s value2", s, s), "field1", "field2");
        testWhereClause(format("field1 %s ? AND field2 %s ?", s, s), "field1", "field2");

        testWhereClause(format("field1 %s value1 OR field2 %s value2", s, s), "field1", "field2");
        testWhereClause(format("field1 %s ? OR field2 %s ?", s, s), "field1", "field2");

        testWhereClause(format("field1 %s value1 AND (field2 %s value2 OR field3 %s value3)", s, s, s), "field1", "field2", "field3");
        testWhereClause(format("field1 %s ? AND (field2 %s ? OR field3 %s ?)", s, s, s), "field1", "field2", "field3");

        testWhereClause(format("field1 %s value1 AND field2 IN (value2,value3,value4)", s), "field1", "field2");
        testWhereClause(format("field1 %s ? AND field2 IN (?,?,?)", s), "field1", "field2");

        testWhereClause(format("field1 %s value1 AND field2 NOT   IN (value2,value3,value4)", s), "field1", "field2");
        testWhereClause(format("field1 %s ? AND field2 NOT  IN (?,?,?)", s), "field1", "field2");

        testWhereClause(format("field1 %s value1 AND field2 BETWEEN value2 AND value3", s), "field1", "field2");
        testWhereClause(format("field1 %s ? AND field2 BETWEEN ? AND ?", s), "field1", "field2");

        testWhereClause(format("CUSTOM_FIELD_ID %s ? AND ISSUE_ID %s ?", s, s), "CUSTOM_FIELD_ID", "ISSUE_ID");

        testWhereClause(format("field1 = value1 AND NOT (field2 %s value2 OR field3 NOT IN (1,2,3))", s), "field1", "field2", "field3");
        testWhereClause(format("field1 = ? AND NOT (field2 %s ? OR field3 NOT IN (?,?,?))", s), "field1", "field2", "field3");

        testWhereClause(
                "((TO_DATE >= ? AND TO_DATE <= ?) OR (FROM_DATE >= ? AND FROM_DATE <= ?) OR (FROM_DATE <= ? AND TO_DATE >= ?)) AND COLLABORATOR = ?",
                "TO_DATE", "TO_DATE", "FROM_DATE", "FROM_DATE", "FROM_DATE", "TO_DATE", "COLLABORATOR");

        testWhereClause(
                "(TO_DATE >= ? AND TO_DATE <= ?)OR (FROM_DATE >= ? AND FROM_DATE <= ?) OR (FROM_DATE <= ? AND TO_DATE >= ?) AND COLLABORATOR IN ('adtest','benediktb','bjarnit','fiddi','hannesj','jonegill','kristin','magnuse','petur','sverrir','vs')",
                "TO_DATE", "TO_DATE", "FROM_DATE", "FROM_DATE", "FROM_DATE", "TO_DATE", "COLLABORATOR");
    }

    private void testWhereClause(String clause, String... fields) {
        final Matcher m = SqlUtils.WHERE_CLAUSE.matcher(clause);
        for (String field : fields) {
            assertTrue("Could not match " + field, m.find());
            assertEquals(field, m.group(1));
        }

        final boolean next = m.find();
        if (next) {
            assertFalse("Found an extra match " + m.group(1), next);
        }
    }

    @Test
    public void testProcessWhereClause() {
        for (String s : SEPARATORS) {
            testProcessWhereClauseWithSeparator(s);
        }
    }

    private void testProcessWhereClauseWithSeparator(String s) {
        testProcessWhereClause(
                format("field %s value", s),
                format("*field* %s value", s));

        testProcessWhereClause(
                format("field %s ?", s),
                format("*field* %s ?", s));

        testProcessWhereClause(
                format("field1 %s value1 AND field2 %s value2", s, s),
                format("*field1* %s value1 AND *field2* %s value2", s, s));

        testProcessWhereClause(
                format("field1 %s ? AND field2 %s ?", s, s),
                format("*field1* %s ? AND *field2* %s ?", s, s));

        testProcessWhereClause(
                format("field1 %s value1 OR field2 %s value2", s, s),
                format("*field1* %s value1 OR *field2* %s value2", s, s));

        testProcessWhereClause(
                format("field1 %s ? OR field2 %s ?", s, s),
                format("*field1* %s ? OR *field2* %s ?", s, s));

        testProcessWhereClause(
                format("field1 %s value1 AND (field2 %s value2 OR field3 %s value3)", s, s, s),
                format("*field1* %s value1 AND (*field2* %s value2 OR *field3* %s value3)", s, s, s));

        testProcessWhereClause(
                format("field1 %s ? AND (field2 %s ? OR field3 %s ?)", s, s, s),
                format("*field1* %s ? AND (*field2* %s ? OR *field3* %s ?)", s, s, s));

        testProcessWhereClause(
                format("CUSTOM_FIELD_ID %s ? AND ISSUE_ID %s ?", s, s),
                format("*CUSTOM_FIELD_ID* %s ? AND *ISSUE_ID* %s ?", s, s));

        testProcessWhereClause(
                format("field1 = value1 AND NOT (field2 %s value2 OR field3 NOT IN (1,2,3))", s),
                format("*field1* = value1 AND NOT (*field2* %s value2 OR *field3* NOT IN (1,2,3))", s));

        testProcessWhereClause(
                format("field1 = ? AND NOT (field2 %s ? OR field3 NOT IN (?,?,?))", s),
                format("*field1* = ? AND NOT (*field2* %s ? OR *field3* NOT IN (?,?,?))", s));

        testProcessWhereClause(
                "((TO_DATE >= ? AND TO_DATE <= ?) OR (FROM_DATE >= ? AND FROM_DATE <= ?) OR (FROM_DATE <= ? AND TO_DATE >= ?)) AND COLLABORATOR = ?",
                "((*TO_DATE* >= ? AND *TO_DATE* <= ?) OR (*FROM_DATE* >= ? AND *FROM_DATE* <= ?) OR (*FROM_DATE* <= ? AND *TO_DATE* >= ?)) AND *COLLABORATOR* = ?");

        testProcessWhereClause(
                "(TO_DATE >= ? AND TO_DATE <= ?)OR (FROM_DATE >= ? AND FROM_DATE <= ?) OR (FROM_DATE <= ? AND TO_DATE >= ?) AND COLLABORATOR IN ('adtest','benediktb','bjarnit','fiddi','hannesj','jonegill','kristin','magnuse','petur','sverrir','vs')",
                "(*TO_DATE* >= ? AND *TO_DATE* <= ?)OR (*FROM_DATE* >= ? AND *FROM_DATE* <= ?) OR (*FROM_DATE* <= ? AND *TO_DATE* >= ?) AND *COLLABORATOR* IN ('adtest','benediktb','bjarnit','fiddi','hannesj','jonegill','kristin','magnuse','petur','sverrir','vs')"
        );
    }

    private void testProcessWhereClause(String where, String expected) {
        assertEquals(expected, SqlUtils.processWhereClause(where, TEST_ID_PROCESSOR));
    }

    @Test
    public void testProcessOnClausePattern() {
        assertEquals("*id* = *otherId*", SqlUtils.processOnClause("id = otherId", TEST_ID_PROCESSOR));
        assertEquals("a.*id* = b.*otherId*", SqlUtils.processOnClause("a.id = b.otherId", TEST_ID_PROCESSOR));
        assertEquals("schema.*a*.*id* = schema.*b*.*otherId*", SqlUtils.processOnClause("schema.a.id = schema.b.otherId", TEST_ID_PROCESSOR));
    }

    @Test
    public void testProcessGroupByClause() {
        assertEquals("*id*,*otherId*", SqlUtils.processGroupByClause("id,otherId", TEST_ID_PROCESSOR));
        assertEquals("*a*.*id*,*b*.*otherId*", SqlUtils.processGroupByClause("a.id,b.otherId", TEST_ID_PROCESSOR));
    }

    private static class TestIdProcessor implements Function<String, String> {
        @Override
        public String apply(String id) {
            return "*" + id + "*";
        }
    }
}
