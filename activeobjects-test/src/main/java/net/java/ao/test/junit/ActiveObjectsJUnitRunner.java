package net.java.ao.test.junit;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.SequenceNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.TriggerNameConverter;
import net.java.ao.test.converters.NameConverters;
import net.java.ao.test.jdbc.Hsql;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.lucene.WithIndex;
import org.junit.rules.MethodRule;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public final class ActiveObjectsJUnitRunner extends BlockJUnit4ClassRunner
{
    private final JdbcConfiguration jdbcConfiguration;
    private final boolean withIndex;
    private TableNameConverter tableNameConverter;
    private FieldNameConverter fieldNameConverter;
    private SequenceNameConverter sequenceNameConverter;
    private TriggerNameConverter triggerNameConverter;
    private IndexNameConverter indexNameConverter;

    public ActiveObjectsJUnitRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
        jdbcConfiguration = resolveJdbcConfiguration(klass);
        tableNameConverter = tableNameConverter(klass);
        fieldNameConverter = fieldNameConverter(klass);
        sequenceNameConverter = sequenceNameConverter(klass);
        triggerNameConverter = triggerNameConverter(klass);
        indexNameConverter = indexNameConverter(klass);
        withIndex = withIndex(klass);
    }

    @Override
    protected List<MethodRule> rules(Object test)
    {
        final LinkedList<MethodRule> methodRules = new LinkedList<MethodRule>(super.rules(test));
        methodRules.add(new ActiveObjectTransactionMethodRule(test, jdbcConfiguration, withIndex, tableNameConverter, fieldNameConverter, sequenceNameConverter, triggerNameConverter, indexNameConverter));
        return methodRules;
    }

    private boolean withIndex(Class<?> klass)
    {
        return klass.isAnnotationPresent(WithIndex.class);
    }

    private TableNameConverter tableNameConverter(Class<?> klass)
    {
        if (klass.isAnnotationPresent(NameConverters.class))
        {
            return newInstance(klass.getAnnotation(NameConverters.class).table());
        }
        return null;
    }

    private FieldNameConverter fieldNameConverter(Class<?> klass)
    {
        if (klass.isAnnotationPresent(NameConverters.class))
        {
            return newInstance(klass.getAnnotation(NameConverters.class).field());
        }
        return null;
    }

    private SequenceNameConverter sequenceNameConverter(Class<?> klass)
    {
        if (klass.isAnnotationPresent(NameConverters.class))
        {
            return newInstance(klass.getAnnotation(NameConverters.class).sequence());
        }
        return null;
    }

    private TriggerNameConverter triggerNameConverter(Class<?> klass)
    {
        if (klass.isAnnotationPresent(NameConverters.class))
        {
            return newInstance(klass.getAnnotation(NameConverters.class).trigger());
        }
        return null;
    }

    private IndexNameConverter indexNameConverter(Class<?> klass)
    {
        if (klass.isAnnotationPresent(NameConverters.class))
        {
            return newInstance(klass.getAnnotation(NameConverters.class).index());
        }
        return null;
    }
    
    private JdbcConfiguration resolveJdbcConfiguration(Class<?> klass)
    {
        if (klass.isAnnotationPresent(Jdbc.class))
        {
            return newInstance(klass.getAnnotation(Jdbc.class).value());
        }
        return getDefaultJdbcConfiguration();
    }

    private <T> T newInstance(Class<T> type)
    {
        try
        {
            return type.newInstance();
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }

    private JdbcConfiguration getDefaultJdbcConfiguration()
    {
        return new Hsql();
    }
}
