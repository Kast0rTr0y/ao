package net.java.ao;

import net.java.ao.test.junit.H2EmbeddedIntegrationTest;
import org.junit.experimental.categories.Category;

@Category (H2EmbeddedIntegrationTest.class)
public class H2EmbeddedQueryTest extends QueryTest
{
    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return DatabaseProviders.getH2DatabaseProvier();
    }

    @Override
    protected String getExpectedSqlForSimpleSelect()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSimpleCount()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectSqlForSelectSomeFields()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectSqlForCountSomeFields()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithWhereClause()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithWhereClause()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithOrderClause()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithOrderClause()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithLimit()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithLimit()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForDistinctSelectWithLimit()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForDistinctSelectWithOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithLimitAndOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithLimitAndOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForDistinctSelectWithLimitAndOffset()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithGroupBy()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithGroupBy()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithExplicitJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithExplicitJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithDefaultJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForCountWithDefaultJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoinAndSomeFields()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedExplicitJoin()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

}
