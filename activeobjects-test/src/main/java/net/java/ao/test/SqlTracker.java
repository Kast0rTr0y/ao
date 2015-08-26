package net.java.ao.test;

import net.java.ao.DatabaseProvider;

import java.util.List;

import static com.google.common.collect.Lists.newLinkedList;

/**
 *
 */
public final class SqlTracker implements DatabaseProvider.SqlListener {
    private final List<String> sqlStatements = newLinkedList();

    public void onSql(String sql) {
        sqlStatements.add(sql);
    }

    public boolean isSqlExecuted() {
        return !sqlStatements.isEmpty();
    }
}
