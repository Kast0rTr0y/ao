package net.java.ao.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import net.java.ao.DatabaseProvider;
import net.java.ao.DisposableDataSource;
import net.java.ao.Query;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.UniqueNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.schema.ddl.SQLAction;
import net.java.ao.types.TypeManager;

import java.sql.Types;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.Iterables.concat;

public class H2DatabaseProvider extends DatabaseProvider {
    public H2DatabaseProvider(final DisposableDataSource dataSource) {
        this(dataSource, "PUBLIC");
    }

    public H2DatabaseProvider(final DisposableDataSource dataSource, final String schema) {
        super(dataSource, schema, TypeManager.h2());
    }

    @Override
    protected String renderQueryLimit(final Query query) {
        final StringBuilder sql = new StringBuilder();

        // H2 requires a LIMIT when OFFSET is specified; -1 indicates unlimited
        if (query.getLimit() < 0 && query.getOffset() > 0) {
            sql.append(" LIMIT -1");
        }

        sql.append(super.renderQueryLimit(query));

        return sql.toString();
    }

    @Override
    protected Iterable<SQLAction> renderAlterTableAddColumn(final NameConverters nameConverters, final DDLTable table, final DDLField field) {
        final Iterable<SQLAction> back = super.renderAlterTableAddColumn(nameConverters, table, field);

        if (field.isUnique()) {
            return concat(back, ImmutableList.of(renderAddUniqueConstraint(nameConverters.getUniqueNameConverter(), table, field)));
        }

        return back;
    }

    @Override
    protected Iterable<SQLAction> renderAlterTableChangeColumn(final NameConverters nameConverters, final DDLTable table, final DDLField oldField, final DDLField field) {
        final ImmutableList.Builder<SQLAction> back = ImmutableList.builder();

        back.addAll(super.renderAlterTableChangeColumn(nameConverters, table, oldField, field));

        if (!field.isPrimaryKey()) {
            if (oldField.isUnique() && !field.isUnique()) {
                back.add(renderDropUniqueConstraint(nameConverters.getUniqueNameConverter(), table, field));
            } else if (!oldField.isUnique() && field.isUnique()) {
                back.add(renderAddUniqueConstraint(nameConverters.getUniqueNameConverter(), table, field));
            }
        }

        return back.build();
    }

    @Override
    protected SQLAction renderAlterTableChangeColumnStatement(final NameConverters nameConverters, final DDLTable table, final DDLField oldField, final DDLField field, final RenderFieldOptions options) {
        final StringBuilder sql = new StringBuilder();

        sql.append("ALTER TABLE ");
        sql.append(withSchema(table.getName()));
        sql.append(" ALTER COLUMN ");
        sql.append(renderField(nameConverters, table, field, options));

        if (oldField.isNotNull() && !field.isNotNull()) {
            sql.append(" NULL ");
        }

        return SQLAction.of(sql);
    }

    @Override
    protected String renderFieldDefault(final DDLTable table, final DDLField field) {
        final StringBuilder sql = new StringBuilder();

        if (field.getDefaultValue() != null) {
            sql.append(" DEFAULT ").append(renderValue(field.getDefaultValue()));
        }

        return sql.toString();
    }

    @Override
    protected SQLAction renderAlterTableDropKey(final DDLForeignKey key) {
        final StringBuilder sql = new StringBuilder();

        sql.append("ALTER TABLE ");
        sql.append(withSchema(key.getDomesticTable()));
        sql.append(" DROP CONSTRAINT ");
        sql.append(processID(key.getFKName()));

        return SQLAction.of(sql);
    }

    @Override
    protected SQLAction renderDropIndex(final IndexNameConverter indexNameConverter, final DDLIndex index) {
        return SQLAction.of(new StringBuilder()
                        .append("DROP INDEX IF EXISTS ")
                        .append(withSchema(index.getIndexName()))
        );
    }

    @Override
    protected String renderConstraintsForTable(final UniqueNameConverter uniqueNameConverter, final DDLTable table) {
        final StringBuilder sql = new StringBuilder(super.renderConstraintsForTable(uniqueNameConverter, table));

        for (final DDLField field : table.getFields()) {
            if (field.isUnique()) {
                sql.append("   ");
                sql.append(renderUniqueConstraint(uniqueNameConverter, table, field));
                sql.append(",\n");
            }
        }

        return sql.toString();
    }

    @Override
    protected String renderUnique(final UniqueNameConverter uniqueNameConverter, final DDLTable table, final DDLField field) {
        return "";
    }

    @Override
    public Object parseValue(final int type, String value) {
        if (value == null || value.equals("") || value.equals("NULL")) {
            return null;
        }

        switch (type) {
            case Types.TIMESTAMP:
            case Types.DATE:
            case Types.TIME:
            case Types.VARCHAR:
                Matcher matcher = Pattern.compile("'(.*)'.*").matcher(value);
                if (matcher.find()) {
                    value = matcher.group(1);
                }
                break;
        }

        return super.parseValue(type, value);
    }

    @Override
    protected Set<String> getReservedWords() {
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

    private SQLAction renderAddUniqueConstraint(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field) {
        final StringBuilder sql = new StringBuilder();

        sql.append("ALTER TABLE ");
        sql.append(withSchema(table.getName()));
        sql.append(" ADD ");
        sql.append(renderUniqueConstraint(uniqueNameConverter, table, field));

        return SQLAction.of(sql);
    }

    private SQLAction renderDropUniqueConstraint(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field) {
        final StringBuilder sql = new StringBuilder();

        sql.append("ALTER TABLE ");
        sql.append(withSchema(table.getName()));
        sql.append(" DROP CONSTRAINT ");
        sql.append(uniqueNameConverter.getName(table.getName(), field.getName()));

        return SQLAction.of(sql);
    }

    private String renderUniqueConstraint(UniqueNameConverter uniqueNameConverter, DDLTable table, DDLField field) {
        final StringBuilder sql = new StringBuilder();

        sql.append(" CONSTRAINT ");
        sql.append(uniqueNameConverter.getName(table.getName(), field.getName()));
        sql.append(" UNIQUE(");
        sql.append(processID(field.getName()));
        sql.append(")");

        return sql.toString();
    }
}
