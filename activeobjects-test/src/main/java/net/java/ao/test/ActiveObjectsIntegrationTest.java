package net.java.ao.test;

import net.java.ao.DatabaseProvider;
import net.java.ao.EntityManager;
import net.java.ao.RawEntity;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

@RunWith(ActiveObjectsJUnitRunner.class)
public abstract class ActiveObjectsIntegrationTest {
    static {
        // disabling length check for AO lib
        System.setProperty("ao.atlassian.enforce.length", String.valueOf(false));
    }

    protected EntityManager entityManager;

    protected final <T> T checkSqlExecuted(Callable<T> callable) throws Exception {
        return DbUtils.checkSqlExecuted(entityManager, callable);
    }

    protected final <T> T checkSqlNotExecuted(Callable<T> callable) throws Exception {
        return DbUtils.checkSqlNotExecuted(entityManager, callable);
    }

    protected final <E extends RawEntity<?>> E checkSqlExecutedWhenSaving(final E entity) throws Exception {
        return DbUtils.checkSqlExecutedWhenSaving(entityManager, entity);
    }

    protected final void executeUpdate(final String sql, DbUtils.UpdateCallback callback) throws Exception {
        DbUtils.executeUpdate(entityManager, sql, callback);
    }

    protected final void executeStatement(final String sql, DbUtils.StatementCallback callback) throws Exception {
        DbUtils.executeStatement(entityManager, sql, callback);
    }

    protected final String getTableName(Class<? extends RawEntity<?>> entityType) {
        return EntityUtils.getTableName(entityManager, entityType);
    }

    /**
     * Get the table name of the given class entity
     *
     * @param entityType the class of the entity
     * @param escape     whether or not to escape the table name
     * @return the table name
     */
    protected final String getTableName(Class<? extends RawEntity<?>> entityType, boolean escape) {
        return EntityUtils.getTableName(entityManager, entityType, escape);
    }

    protected final String getFieldName(Class<? extends RawEntity<?>> entityType, String methodName) {
        return EntityUtils.getFieldName(entityManager, entityType, methodName);
    }

    protected final String getPolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName) {
        return EntityUtils.getPolyFieldName(entityManager, entityType, methodName);
    }

    protected final String escapeFieldName(Class<? extends RawEntity<?>> entityType, String methodName) {
        return EntityUtils.escapeFieldName(entityManager, entityType, methodName);
    }

    protected final String escapePolyFieldName(Class<? extends RawEntity<?>> entityType, String methodName) {
        return EntityUtils.escapePolyFieldName(entityManager, entityType, methodName);
    }

    protected final String escapeKeyword(String keyword) {
        return EntityUtils.escapeKeyword(entityManager, keyword);
    }

    protected final DDLField findField(DDLTable table, String name) {
        for (DDLField field : table.getFields()) {
            if (field.getName().equalsIgnoreCase(name)) {
                return field;
            }
        }
        throw new IllegalStateException("Couldn't find field '" + name + "' in table '" + table.getName() + "'");
    }

    protected final DDLField findField(DDLTable table, Class<? extends RawEntity<?>> entityClass, String methodName) {
        return findField(table, getFieldName(entityClass, methodName));
    }

    protected DDLIndexField field(String name, Class type, Class<? extends RawEntity<?>> entity) {
        return DDLIndexField.builder()
                .fieldName(getFieldName(entity, name))
                .type(entityManager.getProvider().getTypeManager().getType(type))
                .build();
    }

    protected String indexName(String tableName, String indexName) {
        final IndexNameConverter indexNameConverter = entityManager.getNameConverters().getIndexNameConverter();
        final DatabaseProvider provider = entityManager.getProvider();

        return indexNameConverter.getName(provider.shorten(tableName), provider.shorten(indexName));
    }
}
