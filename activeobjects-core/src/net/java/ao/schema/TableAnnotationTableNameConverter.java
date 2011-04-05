package net.java.ao.schema;

import net.java.ao.RawEntity;

import static com.google.common.base.Preconditions.*;

/**
 * <p>Gets the table name from the {@link Table Table annotation}. If no annotation is
 * present on the given entity then it will delegate to the configured {@link TableNameConverter}.</p>
 *
 * @since 0.9
 */
public final class TableAnnotationTableNameConverter implements TableNameConverter
{
    public static final Class<Table> TABLE_ANNOTATION = Table.class;

    private final TableNameConverter delegateTableNameConverter;
    private final CanonicalClassNameTableNameConverter postProcessingTableNameConverter;

    public TableAnnotationTableNameConverter(TableNameConverter delegateTableNameConverter)
    {
        this(delegateTableNameConverter, new IdentityTableNameConverter());
    }

    public TableAnnotationTableNameConverter(TableNameConverter delegateTableNameConverter, CanonicalClassNameTableNameConverter postProcessingTableNameConverter)
    {
        this.delegateTableNameConverter = checkNotNull(delegateTableNameConverter);
        this.postProcessingTableNameConverter = checkNotNull(postProcessingTableNameConverter);
    }

    /**
     * Gets the name of the table either from the {@link Table Table annotation} if present or from the
     * {@link TableNameConverter delegate}.
     *
     * @param entityClass the entity from which to extract the table name
     * @return the table name for the given entity
     * @throws IllegalStateException if the {@link Table Table annotation} value is invalid ({@code null} or
     * empty {@link String})
     */
    public String getName(Class<? extends RawEntity<?>> entityClass)
    {
        if (entityClass.isAnnotationPresent(TABLE_ANNOTATION))
        {
            return postProcessingTableNameConverter.getName(validate(entityClass.getAnnotation(TABLE_ANNOTATION).value()));
        }
        else
        {
            return delegateTableNameConverter.getName(entityClass);
        }
    }

    private String validate(String value)
    {
        checkState(value != null && !value.equals(""), "Value %s for table annotation is not valid.", value);
        return value;
    }

    private static class IdentityTableNameConverter extends CanonicalClassNameTableNameConverter
    {
        @Override
        protected String getName(String entityClassCanonicalName)
        {
            return entityClassCanonicalName;
        }
    }
}
