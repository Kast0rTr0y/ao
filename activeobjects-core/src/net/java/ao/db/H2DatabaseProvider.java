package net.java.ao.db;

import com.google.common.collect.ImmutableSet;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.Query;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
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
    protected String renderQueryLimit(final Query query)
    {
        StringBuilder sql = new StringBuilder();

        // H2 requires a LIMIT when OFFSET is specified; -1 indicates unlimited
        if (query.getLimit() < 0 && query.getOffset() > 0)
        {
            sql.append(" LIMIT -1");
        }

        sql.append(super.renderQueryLimit(query));

        return sql.toString();
    }

    @Override
    protected SQLAction renderAlterTableChangeColumnStatement(final NameConverters nameConverters, final DDLTable table, final DDLField oldField, final DDLField field, final RenderFieldOptions options)
    {
        return SQLAction.of(new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(withSchema(table.getName()))
                        .append(" ALTER COLUMN ")
                        .append(renderField(nameConverters, table, field, options))
        );
    }

    @Override
    protected SQLAction renderAlterTableDropKey(DDLForeignKey key)
    {
        return SQLAction.of(new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(withSchema(key.getDomesticTable()))
                        .append(" DROP CONSTRAINT ")
                        .append(processID(key.getFKName()))
        );
    }

    @Override
    protected SQLAction renderDropIndex(IndexNameConverter indexNameConverter, DDLIndex index)
    {
        return SQLAction.of(new StringBuilder()
                        .append("DROP INDEX IF EXISTS ")
                        .append(withSchema(getExistingIndexName(indexNameConverter, index)))
        );
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
