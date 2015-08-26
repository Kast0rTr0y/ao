package net.java.ao.db;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.java.ao.DatabaseProvider;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;

import static net.java.ao.DatabaseProviders.getPostgreSqlDatabaseProvider;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public final class PostgresDatabaseProviderTest extends DatabaseProviderTest {
    @Test
    public final void testProcessOrderClauseQuoted() {
        final List<String> orderClauses = ImmutableList.of(
                "\"column1\"",
                "\"column1\" ASC",
                "\"table1\".\"column1\"",
                "\"table1\".\"column1\" ASC"
        );

        final List<String> processedOrderClauses = Lists.transform(orderClauses, new Function<String, String>() {
            @Override
            public String apply(@Nullable final String input) {
                return getDatabaseProvider().processOrderClause(input);
            }
        });

        assertThat(processedOrderClauses, is(orderClauses));
    }

    @Override
    protected String getDatabase() {
        return "postgres";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider() {
        return getPostgreSqlDatabaseProvider();
    }

    @Override
    protected String getExpectedWhereClause() {
        return "\"field1\" = 2 and \"field2\" like %er";
    }

    @Override
    protected String getExpectedWhereClauseWithWildcard() {
        // this is invalid SQL but is used to check that field1 is potentially quoted but * isn't
        // PostgreSQL should quote all but wildcards and digits
        return "\"field1\" = *";
    }

    @Override
    protected String getExpectedWhereClauseWithUnderscore() {
        return "\"_field1\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithNumericIdentifier() {
        // PostgreSQL should quote all but wildcards and digits
        return "\"12345abc\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithUnderscoredNumeric() {
        // PostgreSQL should quote all but wildcards and digits
        return "\"_12345abc\" = 1";
    }

    @Override
    protected String getExpectedWhereClauseWithAlphaNumeric() {
        // PostgreSQL should quote all but wildcards and digits
        return "\"a12345bc\" = 1";
    }

    @Override
    protected List<String> getExpectedOrderClauses() {
        return ImmutableList.of(
                "\"column1\"",
                "\"column1\" Asc",
                "\"column1\" Desc",
                "\"table1\".\"column1\"",
                "\"table1\".\"column1\" ASC",
                "\"table1\".\"column1\" ASC, \"column2\"",
                "\"column1\", \"table2\".\"column2\" ASC",
                "\"table1\".\"column1\" ASC, \"table2\".\"column2\" ASC"
        );
    }
}
