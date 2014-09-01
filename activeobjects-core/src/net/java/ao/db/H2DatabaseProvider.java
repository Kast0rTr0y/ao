package net.java.ao.db;

import com.google.common.collect.ImmutableSet;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.types.TypeManager;

import java.util.Set;

public class H2DatabaseProvider extends DatabaseProvider
{
    public H2DatabaseProvider(final DisposableDataSource dataSource)
    {
        super(dataSource, "PUBLIC", TypeManager.h2());
    }

    public H2DatabaseProvider(final DisposableDataSource dataSource, final String schema)
    {
        super(dataSource, schema, TypeManager.h2());
    }

    @Override
    protected Set<String> getReservedWords()
    {
        return RESERVED_WORDS;
    }

    private static final Set<String> RESERVED_WORDS = ImmutableSet.of(
            "CROSS",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "DISTINCT",
            "EXCEPT",
            "EXISTS",
            "FALSE",
            "FOR",
            "FROM",
            "FULL",
            "GROUP",
            "HAVING",
            "INNER",
            "INTERSECT",
            "IS",
            "JOIN",
            "LIKE",
            "LIMIT",
            "MINUS",
            "NATURAL",
            "NOT",
            "NULL",
            "ON",
            "ORDER",
            "PRIMARY",
            "ROWNUM",
            "SELECT",
            "SYSDATE",
            "SYSTIME",
            "SYSTIMESTAMP",
            "TODAY",
            "TRUE",
            "UNION",
            "UNIQUE",
            "WHERE"
    );
}
