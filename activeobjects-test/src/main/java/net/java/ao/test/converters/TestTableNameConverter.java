package net.java.ao.test.converters;

import net.java.ao.RawEntity;
import net.java.ao.schema.Case;
import net.java.ao.schema.TableAnnotationTableNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TestTableNameConverter implements TableNameConverter
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final TableNameConverter tableNameConverter;

    public TestTableNameConverter(Prefix prefix)
    {
        // this the basic conversion we want, under score and upper case
        final UnderscoreTableNameConverter baseConverter = new UnderscoreTableNameConverter(Case.UPPER);

        tableNameConverter =
                new PrefixedTableNameConverter(
                        checkNotNull(prefix),
                        new TableAnnotationTableNameConverter(baseConverter, baseConverter));
    }

    @Override
    public String getName(Class<? extends RawEntity<?>> entityClass)
    {
        final String name = tableNameConverter.getName(entityClass);
        logger.debug("Table name for '{}' is '{}'", entityClass.getName(), name);
        return name;
    }
}
